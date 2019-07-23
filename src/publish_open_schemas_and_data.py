import csv
import datetime
import gzip
import hashlib
import json
import os
import struct
import sys
import uuid
from configparser import ConfigParser

import ipfsApi
import requests
from Crypto.Cipher import AES
# import certifi
from essential_generators import DocumentGenerator
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from sodapy import Socrata


class ReblocMarketplace:
    def __init__(self,url,gq_client):
        self.url = url
        self.client = gq_client

    def look_up_user_id(self,email):
        userid = -1
        # s = requests.Session()
        # s.verify = "./rebloc.io.cert.pem"
        req = requests.get(self.url + '/profile/' + email, verify=False)
        print (req.content)

        if req.status_code == 200:
            profile = json.loads(req.content)
            userid = profile['id']
        else:
            raise Exception('look up marketplace user id faile: {0}'.format(req.content))
        return userid


    def post_draft_dataset(self,dataset):
        print (dataset)
        result = None
        print (self.url)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        req = requests.post(self.url+'/schema',json=dataset,headers=headers,verify=False)
        print (req.status_code)

        if req.status_code == 200:
            return req.content
        else:
            raise Exception('not item found or api server error {0}'.format(result.content))


    def query_published_field(self,ds_id):
        field_name_query = \
            gql('''
                   query datasets ($id: String ) {
                        marketplace_data_source_detail 
                                     ( where:{id:{ _eq: $id}} )
                       {
                           schema
                       }
                     }
                ''')

        params = {
            'source_id': ds_id,
        }
        result = self.client.execute(field_name_query, variable_values=params)
        cols = []
        for row in result['marketplace_source_of_field']['schema']:
            cols.append(row['name'])
        return cols


class MyOpenData:
    def __init__(self,client,metadata,id):
        self.id = id
        self.client = client
        self.metadata = metadata

    def __del__(self):
        ...

    def data_schema(self):
        schema = []
        try:
            for x in self.metadata['columns']:
                col = dict()
                col['name'] = x['fieldName']

                if x['dataTypeName'] == "calendar_date":
                    col['type'] = "timestamp without timezone"
                else:
                    if x['dataTypeName'] == "number":
                        col['type'] = "integer"
                    else:
                        col['type'] = x['dataTypeName']

                col['label'] = x['name'].replace('_', ' ')
                if x.get('description') is None:
                    col['description'] = ""
                else:
                    col['description'] = x['description']

                schema.append(col)

        except Exception as err:
            print (x)
            print (err)

        return schema

    def publish_sample_data(self,cypher_key,ipfs_server,ipfs_port,sample_size=100,output='json'):

        # create sample data
        dataset = self.client.get(self.id,content_type=output,limit=sample_size)
        result_file_name = "/tmp/%s-sample.%s.gz" % (self.id, output)
        out_file = gzip.open(result_file_name, "wt")

        if output == 'csv':
            csv_writer = csv.writer(out_file)
            for row in dataset:
                csv_writer.writerow(row)
        else:
            out_file.write(json.dumps(dataset,indent=4,sort_keys=False,default=str))

        out_file.close()
        md5_hash = create_file_hash(result_file_name)
        enc_file_name = result_file_name + '.enc'
        encrypt_file(cypher_key,result_file_name, enc_file_name)

        api = ipfsApi.Client(ipfs_server, ipfs_port)
        res = api.add(enc_file_name)

        return { 'ipfs_hash': res['Hash'],'md5_file_hash': md5_hash }

    # json format is not suitable for full dataset as its size could be millions of rows
    def publish_all_data(self,cypher_key,ipfs_server,ipfs_port):

        result_file_name = "/tmp/%s.%s.gz" % (self.id, "csv")
        out_file = gzip.open(result_file_name, "wt")
        csv_writer = csv.writer(out_file)
        total_records = 1
        # get the first record and header
        dataset = self.client.get(self.id, content_type='csv', limit=1, offset=0)
        for row in dataset:
            csv_writer.writerow(row)
        out_file.flush()

        set_limit = 10000

        while True:
            dataset = self.client.get(self.id, content_type='csv', limit=set_limit, offset=total_records)
            # minus the header row
            total_records += len(dataset) - 1

            # no data only header
            # if len(dataset) == 1:
            #     break

            # for testing purpose fetch only 300000 records
            if len(dataset) == 1 or total_records > 300001:
                break

            for row in dataset[1:]:
                csv_writer.writerow(row)
            out_file.flush()

        out_file.close()
        md5_hash=create_file_hash(result_file_name)

        enc_file_name = result_file_name + '.enc'
        encrypt_file(cypher_key, result_file_name, enc_file_name)

        api = ipfsApi.Client(ipfs_server, ipfs_port)
        res = api.add(enc_file_name)

        return { 'ipfs_hash': res['Hash'],'md5_file_hash': md5_hash, 'num_of_rows': total_records }


def create_file_hash(filename):
    md5_hash = hashlib.md5()
    with open(filename,"rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            md5_hash.update(byte_block)
    f.close()
    print("MD5 Hash of the file %s" % md5_hash.hexdigest())
    return md5_hash.hexdigest()


def encrypt_file(key,in_filename,out_filename=None, chunksize=64*1024):
    if not out_filename:
        out_filename = in_filename + '.enc'

    # iv = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
    iv = os.urandom(16)
    # print ("iv = %s, size = %d" % (str(iv) ,len(iv)))
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    filesize = os.path.getsize(in_filename)

    with open(in_filename, 'rb') as infile:
        with open(out_filename, 'wb') as outfile:
            outfile.write(struct.pack('<Q', filesize))
            outfile.write(iv)

            while True:
                chunk = infile.read(chunksize)
                if len(chunk) == 0:
                    break
                elif len(chunk) % 16 != 0:
                    chunk += ' '.encode('utf8') * (16 - len(chunk) % 16)

                outfile.write(encryptor.encrypt(chunk))


def config(filename='.connection-nycopen.ini',section='graphql'):
    # create a parser
    parser = ConfigParser()

    # read config file
    parser.read(filename)

    # get section, default to postgresql
    info = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            info [param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return info


def main (args):
    if len(args) == 0:
        print ('args: list of dataset id, state/province, city, country')
        exit(0)

    ids = []
    with open(args[0],'r') as id_file:
        for line in id_file:
            ids.append(line.strip('\n\r'))

    print (ids)

    graphql = config(section='graphql')
    rebloc = config(section='rebloc')

    headers = { 'X-Hasura-Access-Key': graphql['apitoken']}
    _transport = RequestsHTTPTransport(
                    url=graphql['endpoint'],
                    headers=headers,
                    use_json=True
                )

    graphql_client = Client(
                        transport=_transport,
                        fetch_schema_from_transport=True
                    )

    my_marketplace = ReblocMarketplace(rebloc['endpoint'],graphql_client)
    ownerid = my_marketplace.look_up_user_id(rebloc['registeremail'])

    api_config = config(section='sourceapi')
    domain_client = Socrata(api_config['domain'], api_config['token'])

    for dataset_identifier in ids:
        metadata = domain_client.get_metadata(dataset_identifier)

        open_data = MyOpenData(domain_client,metadata,dataset_identifier)

        try:
            schema = open_data.data_schema()
            print (schema)

            server_config = config(section='ipfs')
            gen = DocumentGenerator()

            seed = gen.sentence()
            print(seed)
            # 32 bytes encryption keys
            sample_key = hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32].encode('utf8')
            print("key = %s" % sample_key)

            seed = gen.sentence()
            print(seed)
            # 32 bytes encryption keys
            data_key = hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32].encode('utf8')
            print("key = %s" % data_key)

            # publish sample
            print ('publishing sample....')
            sample_info = open_data.publish_sample_data(
                                        sample_key,
                                        server_config['endpoint'],
                                        server_config['port'],
                                        sample_size=300
                                    )

            # publish full data
            print('publishing all data....')
            data_info = open_data.publish_all_data(
                                        data_key,
                                        server_config['endpoint'],
                                        server_config['port']
                                    )

            current_date_time = datetime.datetime.utcnow().strftime("%a %b %d %H:%M:%S %Y")
            search_terms = "{assets}"
            if metadata.get('tags') is not None:
                search_terms = "{" + ",".join(metadata['tags']) + "}"

            default_ipfs_gateway = "http://demo-app.rebloc.io:8080/ipfs/"
            default_price = 0.5

            if 0.00001 * data_info['num_of_rows'] > default_price:
                default_price = round (0.01 * data_info['num_of_rows'],2)

            dataset = {
                "id": str(uuid.uuid1()),
                "name": metadata['name'],
                "table_name": metadata['id'],
                "description":  metadata['description'],
                "country": "united states",
                "state_province": "new york",
                "city": "{new york}",
                "topic": "{" + "building" + "}",
                "date_created": current_date_time,
                "date_modified": current_date_time,
                "dataset_owner_id": ownerid,
                "delivery_method": "IPFS",
                "enc_data_key": data_key.decode(),
                "enc_sample_key": sample_key.decode(),
                "sample_access_url": default_ipfs_gateway + sample_info['ipfs_hash'],
                "sample_hash": sample_info['md5_file_hash'],
                "access_url": default_ipfs_gateway + data_info['ipfs_hash'],
                'data_hash': data_info['md5_file_hash'],
                "num_of_records": data_info['num_of_rows'],
                "search_terms": search_terms,
                "price_high": default_price,
                "price_low": 0.5,
                "stage": 3,
                "schema": schema,
                "json_schema": json.dumps(schema)
            }
            print (dataset)
            # list draft datasets to marketplace
            result = my_marketplace.post_draft_dataset(dataset)
            print (result)
            print (dataset_identifier + ' completed')

        except Exception as err:
            print("error occurs:%s" % err)

    print ('done')

if __name__ == "__main__":
    main(sys.argv[1:])

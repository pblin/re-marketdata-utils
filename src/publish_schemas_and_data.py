from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from configparser import ConfigParser
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import sys
import uuid
import requests
import json, csv
import datetime
import hashlib
import gzip
import os, random, struct
from Crypto.Cipher import AES
import ipfsApi
import certifi
from essential_generators import DocumentGenerator

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
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        req = requests.post(self.url+'/schema',data=dataset,headers=headers,verify=False)
        print (req.status_code)

        if req.status_code == 200:
            return req.content
        else:
            raise Exception('not item found or api server error {0}'.format(result.content))


    def query_published_field(self,ds_id):
        field_name_query = \
            gql('''
                   query get_fields_published ($dataset_id: String) {
                        marketplace_source_of_field
                            (where: { source_id: { _eq: $dataset_id} } 
                          ) {
                               field_name
                         }
                    }
                ''')

        params = {
            'source_id': ds_id,
        }
        result = self.client.execute(field_name_query, variable_values=params)
        cols = []
        for row in result['marketplace_source_of_field']:
            cols.append(row['field_name'])
        return cols


class CherreData:
    def __init__(self,db_params):
        self.connection = psycopg2.connect(**db_params)
        self.connection.set_client_encoding('UTF8')

    def __del__(self):
        self.connection.close()

    def query_available_tables(self):
        query = "select distinct table_name from cherre_sample_data.table_view where table_name != 'table_view'"
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        list = []
        for row in result:
            list.append(row[0])
        return list


    def table_schema(self,tb_name):
        query = "select column_name, data_type from cherre_sample_data.table_view where table_name = '%s'" % tb_name
        print (query)
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        schema = []

        for row in result:
            field = dict ()
            name = str(row[0])
            field['name'] = name
            field['type'] = row[1]
            field['label'] = name.replace('_',' ').title()
            field['description'] = name.replace('_',' ').title()
            schema.append(field)

        cursor.close()
        return schema


    def publish_sample_data(self,table_name,cypher_key,ipfs_server,ipfs_port,sample_size=100,cols=None,output='json'):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # create sample data
        if cols is None:
            # get all columns
            query = "select * from cherre_sample_data.%s " % table_name + "limit %s" % sample_size
            print (query)
            cursor.execute(query)
        else:
            select_query = "select {} from cherre_sample_data.%s " % table_name + "limit %s" % sample_size
            limit_query = sql.SQL(select_query).format(sql.SQL(', ').join(map(sql.Identifier, cols)))
            print(limit_query.as_string(self.connection))
            cursor.execute(limit_query)

        rows = cursor.fetchall()
        json_str = json.dumps(rows, indent=4, sort_keys=False, default=str)
        result_file_name = "/tmp/%s.%s.gz" % (id, output)
        out_file = gzip.open(result_file_name, "w")

        if output == 'csv':
            csv_writer = csv.DictWriter(out_file, fieldnames=cols)
            csv_writer.writeheader()
            for row in rows:
                csv_writer.writerow(row)
        else:
            out_file.write(json_str.encode('utf8'))

        out_file.close()
        md5_hash = create_file_hash(result_file_name)
        enc_file_name = result_file_name + '.enc'
        encrypt_file(cypher_key,result_file_name, enc_file_name)

        api = ipfsApi.Client(ipfs_server, ipfs_port)
        res = api.add(enc_file_name)

        return { 'ipfs_hash': res['Hash'],'md5_file_hash': md5_hash }


    def publish_all_data(self,table_name,cypher_key,ipfs_server,ipfs_port,cols=None,output='json'):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if cols is None:
            # get all columns
            query = "select * from cherre_sample_data.%s " % table_name
            print(query)
            cursor.execute(query)
        else:
            select_query = "select {} from cherre_sample_data.%s " % table_name
            limit_query = sql.SQL(select_query).format(sql.SQL(', ').join(map(sql.Identifier, cols)))
            print(limit_query.as_string(self.connection))
            cursor.execute(limit_query)

        rows = cursor.fetchall()

        json_str = json.dumps(rows, indent=4, sort_keys=False, default=str)
        result_file_name = "/tmp/%s.%s.gz" % (id, output)
        out_file = gzip.open(result_file_name, "w")

        if output == 'csv':
            csv_writer = csv.DictWriter(out_file, fieldnames=cols)
            csv_writer.writeheader()
            for row in rows:
                csv_writer.writerow(row)
        else:
            out_file.write(json_str.encode('utf8'))

        out_file.close()
        md5_hash=create_file_hash(result_file_name)

        enc_file_name = result_file_name + '.enc'
        encrypt_file(cypher_key, result_file_name, enc_file_name)

        api = ipfsApi.Client(ipfs_server, ipfs_port)
        res = api.add(enc_file_name)

        return { 'ipfs_hash': res['Hash'],'md5_file_hash': md5_hash, 'num_of_rows': len(rows) }


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


def config(filename='.connection.ini',section='graphql'):
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

    db_params = config(section='postgresql')
    cherre_data = CherreData(db_params)

    #available data
    list_of_tables = cherre_data.query_available_tables()

    try:
        for table in list_of_tables:
            schema = cherre_data.table_schema(table)
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
            sample_info = cherre_data.publish_sample_data(
                                        table,
                                        sample_key,
                                        server_config['endpoint'],
                                        server_config['port'],
                                        sample_size=300
                                    )

            # publish full data
            print('publishing all data....')
            data_info = cherre_data.publish_all_data(
                                        table,
                                        data_key,
                                        server_config['endpoint'],
                                        server_config['port']
                                    )

            dataset_name = table.replace('_', ' ').title()
            current_date_time = datetime.datetime.utcnow().strftime("%a %b %d %H:%M:%S %Y")
            search_terms = "{" + table.replace('_',',') + "}"
            default_ipfs_gateway = "http://demo-app.rebloc.io:8080/ipfs/"
            default_price = 1.0

            if 0.01 * data_info['num_of_rows'] > default_price:
                default_price = round (0.01 * data_info['num_of_rows'],2)

            dataset = {
                "id": str(uuid.uuid1()),
                "name": dataset_name,
                "table_name": table,
                "description": " Data about " + dataset_name,
                "country": "USA",
                "state_province": "NEW YORK",
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
                "json_schema": json.dumps(schema)
            }

            # list draft datasets to marketplace
            result = my_marketplace.post_draft_dataset(json.dumps(dataset))
            print (result)

    except Exception as err:
        print("error occurs:%s" % err)

if __name__ == "__main__":
    main(sys.argv[1:])

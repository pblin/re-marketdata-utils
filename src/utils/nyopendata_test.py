import os
from sodapy import Socrata
import json

socrata_token = os.environ.get("SODAPY_APPTOKEN")

nyc_realestate_domain = 'data.cityofnewyork.us'
nyc_realestate_dataset_identifier = 'm8p6-tp4b'

nyc_realestate_domain_client = Socrata(nyc_realestate_domain, socrata_token)
metadata = nyc_realestate_domain_client.get_metadata(nyc_realestate_dataset_identifier)
# print (metadata)
dataCols=[]
for x in metadata['columns']:
    col = dict ()
    col['name'] =x['fieldName']

    if x['dataTypeName'] == "calendar_date":
        col['type'] = "timestamp without timezone"
    else:
        if x['dataTypeName'] == "number":
            col['type'] = "integer"
        else:
            col['type'] = x['dataTypeName']


    col['label'] = x['name'].replace('_', ' ')
    col['description'] = x['description']
    dataCols.append(col)

# print (json.dumps(dataCols,indent=4, sort_keys=False,default=str))
dataset = nyc_realestate_domain_client.get(nyc_realestate_dataset_identifier,content_type="csv",limit=1,offset=0)
print (dataset[0])
for row in dataset[1:]:
    print (row)

dataset = nyc_realestate_domain_client.get(nyc_realestate_dataset_identifier,content_type="csv",limit=1,offset=1)
print (dataset[1:])
print (len(dataset))







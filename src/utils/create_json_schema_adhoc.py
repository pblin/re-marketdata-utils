import xlrd
import sys
import json

filename = "Toronto Building - Pool Permits"
loc = ("/Users/bernardlin/Downloads/Toronto_Building_-_Pool_Permits_Readme_File.xls")
gzfile = "clearedpermits2019.csv.gz"

dataset = {
    "name":"Toronto Building - Pool Permits",
    "description": "Toronto Building - Pool Permits",
    "city":"{toronto}",
    "state": "ontario",
    "country":"canada",
    "schema":[],
    "file_path":"poolpermits.csv.gz"
}

wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_name('Sheet1')
schemaJson =open (filename + ".json", "wt")
for i in range (2,16):
    field = {"name": "", "type": "", "label":"","description": ""}
    field["name"] = sheet.cell_value(i, 0).lower()
    field["label"] = field["name"].lower().replace('_',' ').title()
    field["type"] = sheet.cell_value(i, 1)
    field["description"] = sheet.cell_value(i, 2).replace('\t',' ').replace ('"','U+0022').replace('\n',' ')
    dataset['schema'].append (field)

print (json.dumps(dataset))
schemaJson.write (json.dumps(dataset))
schemaJson.flush()
schemaJson.close()


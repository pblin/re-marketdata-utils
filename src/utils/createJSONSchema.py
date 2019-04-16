import xlrd
import sys
import json

loc = ("/Users/bernardlin/Downloads/Cherre_Data_Dictionary_Master-test.xlsx")

wb = xlrd.open_workbook(loc)
sheetNames=wb.sheet_names()

for tableName in sheetNames:

    sheet = wb.sheet_by_name(tableName)
    schemaJson =open (tableName + ".json", "w")

    print ("%s total rows -> " % tableName + str(sheet.nrows) )

    schema =  {
        'schema_name': tableName,
        'fields': []
    }

    for i in range (1,sheet.nrows):
        field = {"name": "", "type": "", "label":"","description": ""};
        if str(sheet.cell_value(i, 1)).find("_id") == -1:
            field["name"] = sheet.cell_value(i, 1)
            field["label"] = sheet.cell_value(i, 2)
            if sheet.cell_value(i, 0) != "USER-DEFINED" :
                field["type"] = sheet.cell_value(i, 0)
            else:
                field["type"] = "text"
            field["description"] = sheet.cell_value(i, 3).replace('\t',' ').replace ('"','U+0022').replace('\n',' ')
            schema['fields'].append (field)

    schemaJson.write (json.dumps(schema))


from __future__ import print_function
import json
import requests
from httplib2 import Http
from oauth2client import file, client, tools
import  psycopg2
from configparser import ConfigParser
from psycopg2 import sql
import xlrd

from googleapiclient.discovery import build

def config(filename='.database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()

    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def insert_fields (conn):

    loc = ("/Users/bernardlin/Downloads/Cherre_Data_Dictionary_Master-test.xlsx")
    wb = xlrd.open_workbook(loc)
    sheetNames=wb.sheet_names()

    for object_name in sheetNames:
        sheet = wb.sheet_by_name(object_name)
        for i in range(1, sheet.nrows):
            # name, type, label, description, object, category, city, county, region, country

            valVec = ['','','','','','','new york','new york','new york','united states']
            if str(sheet.cell_value(i, 1)).find("_id") == -1:
                valVec[0] = sheet.cell_value(i, 1)
                valVec[1] = 'text' #default
                if sheet.cell_value(i, 0) != "USER-DEFINED":
                    valVec[1] = sheet.cell_value(i, 0)
                valVec[2] = sheet.cell_value(i, 2)

                valVec[3] = sheet.cell_value(i, 3).replace('\t', ' ').replace('"', 'U+0022').replace('\n',' ')
                valVec[4] = object_name # object
                valVec[5] = sheet.cell_value(i, 4) # catetory
                print (valVec)
                columns = ["name","type","label","description","object_name",\
                           "category","city","county","region","country"]
                tableInsertStr = "insert into marketplace.field({}) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                insertSql = sql.SQL(tableInsertStr).format(sql.SQL(', ').join(map(sql.Identifier,columns)))

                print (insertSql.as_string(conn))
                cursor.execute(insertSql, valVec)
                conn.commit()

try:
    params = config()

    connection = psycopg2.connect(**params)
    connection.set_client_encoding('UTF8')

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")

    insert_fields(connection)

except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


from __future__ import print_function
import json
import requests
from httplib2 import Http
from oauth2client import file, client, tools
import  psycopg2
from configparser import ConfigParser
from psycopg2 import sql

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

    cursor = conn.cursor()
    # Setup the Sheets API
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    store = file.Storage("token.json")
    creds = store.get()

    data_labels = ['data_type','field_name','name','long_description','category']

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials-bernard918.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))

    # Call the Sheets API
    SPREADSHEET_ID = '17SuLuPP0HwKfyCjNWaidG6ZS0YDCuet_tCLNHJ6IZBs'
    worksheets = ['MVP-1']
    field_inserted = []
    count = 2
    for currentsheet in worksheets:
        RANGE_NAME = currentsheet + '!A2:E275'
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,
                                                 range=RANGE_NAME).execute()


        values = result.get('values', [])
        if not values:
            print('No data found.')
        else:
            #print('object', 'data_type', 'field_name', 'name', 'description')

            assert isinstance(values, object)

            # print (values)
            for row in values:

                field = row[1].lower()
                if field in field_inserted:
                    continue

                field_inserted.append(field)
                #print (len(row))
                # replace single and double quote in description

                if row[2] == 'NA':
                    row[2] = row[1].replace('_',' ').title()

                if row[3] != 'NA':
                    fieldDescription = row[3].replace("'", 'U+0027')
                else:
                    fieldDescription = row[2]

                print(count)
                valVec = [ row[1],row[0],row[2],fieldDescription,row[4],'1' ]
                print (valVec)
                columns = ["name","type","label","description","category","context_id"]

                tableInsertStr = "insert into marketplace.field ({}) values ({})"
                insertSql = sql.SQL(tableInsertStr).format(sql.SQL(', ').join(map(sql.Identifier, columns)), \
                                                           sql.SQL(', ').join(sql.Placeholder() * len(columns)))

                print ( insertSql.as_string(conn))
                cursor.execute(insertSql, valVec)

                # cursor.execute("""INSERT INTO marketplace.field (name,type,label,description,context_id)
                #   VALUES (%s, %s, %s, %s, 1)""", (row[1],row[0],row[2],fieldDescription))
                conn.commit()
                count += 1

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


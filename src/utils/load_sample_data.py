import psycopg2
from psycopg2 import sql
import csv
import codecs
import sys
from ast import literal_eval
from configparser import ConfigParser
import re



def get_field_name_type (conn):
    cursor = conn.cursor()
    cursor.execute ('SELECT distinct name, type from marketplace.field order by name')
    rows = cursor.fetchall()
    fieldType = dict()
    for row in rows:
        fieldType[row[0]] = row[1].lower()

    return fieldType


def create_table(columnList, tablenName, types, conn):


    createTableStatement = "CREATE TABLE " + tablenName + " ( "

    for column in columnList[0:-1]:
        if types.get(column) is not None:
            dataType = types.get(column)
        else:
            dataType = 'text'

        createTableStatement += column + ' ' + dataType  + ' ,'

    # append the last column
    dataType = types.get(columnList[-1])
    if types.get(column) is None:
        dataType = 'text'

    createTableStatement += columnList[-1] + ' ' + dataType  + ' )'

    print ( createTableStatement )
    cursor = conn.cursor()
    cursor.execute (createTableStatement)
    conn.commit()


def insert_data (table, columns, row, types, conn):
    #print ( columns )
    numberType = ['integer','numeric','bigint','smallint','double precision']
    tableInsertStr = "insert into " + table + " ({}) values ({})"
    insertSql = sql.SQL(tableInsertStr).format( sql.SQL(', ').join(map(sql.Identifier, columns)), \
                                                               sql.SQL(', ').join(sql.Placeholder() * len(columns)))

    # print ( insertSql.as_string(conn))

    cursor = conn.cursor()
    valVec = [x for x in row]

    i = 0
    for item in columns:
        columnType = types.get(item)
        if valVec[i] == None or valVec[i]=='':
            if columnType == 'text':
                valVec[i] = ''
            else:
                valVec[i] = None
        else:
            if columnType == 'timestamp':
                 valVec[i] = re.sub('GMT-.*$','', valVec[i])

        i += 1

    # print (valVec)

    cursor.execute (insertSql,valVec)
    conn.commit()


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

def main(argList):
    connection = None
    cursor = None
    file = None
    types = dict()
    rowDBvalues = dict()
    manifest = None

    try:
        params = config()

        connection = psycopg2.connect(**params)
        connection.set_client_encoding('UTF8')

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")
        connection.commit()

        with open (argList[0], "r") as manifest:
            for line in manifest:
                line = line.strip()
                exceptionFile = line.replace('.', '.exception.')
                csvException = csv.writer(open(exceptionFile, "w"))

                with open(line, "r") as file:
                    sniffdialect = csv.Sniffer().sniff(file.readline(), delimiters=',\t')
                    file.seek(0)
                    data = csv.reader(file, sniffdialect)
                    listOfRows = list(data)
                    listRange = len(listOfRows)
                    headers = listOfRows[0]
                    csvException.writerow(headers)
                    # print (headers);
                    tableName = line.split('/')[-1].split('.')[0]
                    tableName = "cherre_sample_data" + '.' + tableName
                    types = get_field_name_type (connection)

                    create_table (headers, tableName, types, connection)

                    for i in range (1, listRange):
                        #skip mismatch rown with non-matching columns
                        if (len(headers) != len(listOfRows[i])):
                            # print (listOfRows[i])
                            csvException.writerow(listOfRows[i])
                            continue
                        # replace quote ' with escape
                        aRow = map (lambda x: x.replace("'", "U+0027"), listOfRows[i])

                        # put ' ' around string
                        #rowStr = ','.join(map(lambda x: "'" + x + "'",aRow))
                        insert_data (tableName,headers, aRow, types, connection)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    except csv.Error:
        print("CSV reading error")
    except ValueError:
        print ("Value assign Eror")
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

        if (file):
            file.close()
        if (manifest):
            manifest.close()


if __name__ == "__main__":
    main(sys.argv[1:])
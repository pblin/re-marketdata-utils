import psycopg2
import csv
import sys

from configparser import ConfigParser


def get_field_name_type (conn):
    cursor = conn.cursor()
    cursor.execute ('SELECT distinct name, type from marketplace.field order by name')
    rows = cursor.fetchall()
    fieldType = dict()
    for row in rows:
        fieldType[row[0]] = row[1]

    return fieldType



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

def main (argList):
    connection = None
    cursor = None
    file = None
    needTypes = None
    manifest = None
    needTypelist = []

    try:

        params = config()

        connection = psycopg2.connect(**params)

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")
        types = get_field_name_type(connection)

        needTypes = open("needTypes.txt", "w")
        with open (argList[0], "r") as manifest:
            for line in manifest:
                line = line.strip()

                with open(line, "r") as file:
                    sniffdialect = csv.Sniffer().sniff(file.readline(), delimiters=',\t')
                    file.seek(0)
                    data = csv.reader(file, sniffdialect)
                    listOfRows = list(data)
                    headers = listOfRows[0]
                    # print (headers);
                    for col in headers:
                        if types.get(col) is None:
                            if col not in needTypelist:
                                needTypelist.append(col)

        for item in needTypelist:
            needTypes.write(item+'\n')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
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

        if (needTypes):
            needTypes.flush()
            needTypes.close()

        if (manifest):
            manifest.close()

if __name__ == "__main__":
    main(sys.argv[1:])
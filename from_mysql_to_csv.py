#!/usr/bin/env python

# Generate csv file from results of a database query
# print is python 2.7 compatible only

# TODO:
# 1. Ideally successful conditions should be folded into try/except vs sys.exit usage

import mysql.connector
import sys
import csv
import ConfigParser
import argparse

# potentially frequently changes details as config filename and output csv file name are runtime options
# variables expected to be changed less frequently as in config file
parser = argparse.ArgumentParser(description='Generate csv file from results of a database query')
parser.add_argument('-c','--config', required=True, help='path to configuraion file')
parser.add_argument('-o','--outputcsv', required=True, help='path to resulting output configuration file')
try:
    args = parser.parse_args()
except:
    parser.print_help()
    sys.exit(1)

# read mysql db connection details and query form config file
config = ConfigParser.RawConfigParser()
try:
    config.read(args.config)
except:
    print "Error occured during configuration file reading"
    sys.exit(1)

# Error out if some of connection settings is not populated
try:
    mysql_hostname=config.get('MySQL', 'hostname')
    mysql_user=config.get('MySQL', 'user')
    mysql_password=config.get('MySQL', 'password')
    mysql_database=config.get('MySQL', 'database')
    mysql_query=config.get('MySQL', 'query')
except ConfigParser.NoSectionError as err:
    print "Error: {0}".format(err)
    sys.exit(1)
except ConfigParser.NoOptionError as err:
    print "Error: {0}".format(err)
    sys.exit(1)
except:
    print "Unknown error"
    sys.exit(1)


# establish mysql connection
try:
    conn = mysql.connector.connect(user=mysql_user, password=mysql_password,host=mysql_hostname,database=mysql_database)
except mysql.connector.ProgrammingError as err:
    print "Error: {0}".format(err)
    sys.exit(1)
cur = conn.cursor()
query = (mysql_query)
try:
    cur.execute(query)
except mysql.connector.ProgrammingError as err:
    print "Error: {0}".format(err)
    cur.close()
    conn.close()
    sys.exit(1)

host_stypes = []
columns = ['hostname','type']

# Fetch all results into dictionary. Who know maybe we will need flexibility later
try:
    for row in cur.fetchall():
        host_stypes.append(dict(zip(columns, row)))
except:
    print "Error occured during results fetch"

# generate csv file
host_stype_file = args.outputcsv
try:
    with open(host_stype_file, mode='w') as host_stype_file_handler:
        host_stype_file_writer = csv.writer(host_stype_file_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        host_stype_file_writer.writerow(['Hostname','Type'])
        for host_stype in host_stypes:
            host_stype_file_writer.writerow([host_stype['hostname'],host_stype['type']])
except:
    print "Error occured during CSV file generation"

cur.close()
conn.close()

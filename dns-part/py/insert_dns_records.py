#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import sys

con = None

def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f)))

try:
    print "Connecting..."
    con = psycopg2.connect(database='tds', user='tds', password='9bBJPLr9')
    print "Connected"
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    files = absoluteFilePaths("../spark/res/webfiles/20130901/")
    #Drop old table if exists
    cursor.execute("DROP TABLE IF EXISTS dns_20130901;")
    #Then Create a new one
    cursor.execute("CREATE TABLE dns_20130901(ts timestamp without time zone, orig_h inet, resp_h inet, query character varying(256), rcode character varying(2), answers text[], TTLs float[]);")
    con.commit

    for file in files:
        lines = open( file, "r" )
        print "adding %s" % file
        for line in lins:
            arr = line.split(' ')
            ts = int(float(arr[0]))
            orig_h = arr[1]
            resp_h = arr[2]
            query = arr[3]
            recode = arr[4]
            answersArr = arr[5].split(',')
            answers = "{"
            for answer in answersArr:
                answers = answers + "\"" + answer + "\","
            answers = answers[:-1] + "}"
            ttls = "{"+arr[6]+"}"

            string = "INSERT INTO dns_20130901 VALUES(to_timestamp(%s), '%s', '%s', '%s', '%s', '%s', '%s');" % (ts, orig_h, resp_h, query, recode, answers, ttls)
            cursor.execute(string)
        lines.close()
        con.commit()
    
except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

finally:
    if con:
        con.close()

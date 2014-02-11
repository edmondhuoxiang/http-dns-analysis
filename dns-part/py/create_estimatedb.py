#!/usr/bin/env python

'''
Created on 02/09/2014

@author: Xiang Huo 
'''

import sys
import os
import redis
import time
from datetime import datetime, timedelta
import psycopg2 as pg
import glob
import gzip
import logging as Log

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./estimate.log',format='%(asctime)s %(message)s', level=Log.INFO)

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()
    cur.execute('insert into estimate_20131001 (domain, count) select query, count(distinct ts) as count from dns_20131001 where rcode != \'-\' and ttls >=0 group by query order by count desc;')
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    sys.exit(1)


def main():
    create_new_table = '''CREATE TABLE %s
    (id serial primary key NOT NULL, domain character varying(256), count int, rate numeric, estimated_vol numeric, distance integer):'''
    data_to_process = '20131001'
    estimate_tname = 'estimate_' + data_to_process
    try:
        cur.execute('DROP TABLE IF EXISTS %s;' % estimate_tname)
        cur.execute(create_new_table % estimate_tname)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (estimate_tname, e.pgerror))
        sys.exit(1)

    try: 
        cur.execute('insert into %s (domain, count) select query, count(distinct ts) as count from dns_20131001 where rcode != \'-\' and ttls >=0 group by query order by count desc;' % estimate_tname)
    except pg.DatabaseError, e:
        Log.error(e.pgerror)
        sys.exit(1)

if__name__ == '__main__':main()

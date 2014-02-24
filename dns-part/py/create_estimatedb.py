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
import pytz

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./estimate.log',format='%(asctime)s %(message)s', level=Log.INFO)

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()
    #cur.execute('insert into estimate_20131001 (domain, count) select query, count(distinct ts) as count from dns_20131001 where rcode != \'-\' and ttls >=0 group by query order by count desc;')
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    sys.exit(1)

def getTimeWindowOfDay(date, tz):
    timezone = pytz.timezone(tz)
    begin = date + ' 0:0:0'
    end = date + ' 23:59:59'
    date_begin = datetime.strptime(begin, "%Y%m%d %H:%M:%S")
    date_end = datetime.strptime(end, "%Y%m%d %H:%M:%S")
    date_begin_localized = timezone.localize(date_begin, is_dst=None)
    date_end_localized = timezone.localize(date_end, is_dst=None)
    return [time.mktime(date_begin_localized.timetuple()), time.mktime(date_end_localized.timetuple())]

def get_query_count(domain, tname):
    data_to_process = tname[:-8]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    try:
        cur.execute('SELECT * FROM %f WHERE query = \'%s\' AND ttls >=0 AND rcode != \'-\' AND ts > %s AND ts < %s ORDER BY orig_h AND ts ASC;' %(tname, domain, tw[0], tw[1]))
        query = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error(e.pgerror)
        sys.exit(1)

    index = 0
    while index < len(res)-1:
        ts1 = res[index]['ts']
        ts2 = res[index+1]['ts']
        if (ts_2-ts_1)<1:
            del(res[index+1])
        else:
            index = index + 1
    return len(res)

def insert(domains, tname, estimate_tname):
    for domain in domains:
        print domain
        count = get_query_count(domain, tname)
        try: 
            cur.execute('INSERT INTO %s (domain, count) values(\'%s\', %s);'%(estimate_tname, domain,count))
        except pg.DatabaseError, e:
            Log.error(e.pgerror)
            sys.exit(1)

def main():
    create_new_table = '''CREATE TABLE %s
    (id serial primary key NOT NULL, domain character varying(256), count int, rate_1 numeric, rate_2 numeric, estimated_vol numeric, distance integer);'''
    data_to_process = '20131001'
    estimate_tname = 'estimate_' + data_to_process
    dns_tname = 'dns_' + data_to_process
    try:
        cur.execute('DROP TABLE IF EXISTS %s;' % estimate_tname)
        cur.execute(create_new_table % estimate_tname)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (estimate_tname, e.pgerror))
        sys.exit(1)
    domains = []
    ins = open("top500", "r")
    for line in ins:
        domains.append(line[:-1])
    insert(domains, dns_tname, estimate_tname)
if __name__ == '__main__':main()

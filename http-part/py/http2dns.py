#!/usr/bin/env python

'''
Created on 01/25/2014

@author: Xiang Huo
'''

import sys
import os
import redis
import time
from datetime import datetime, timedelta
import psycopg2 as pg
import psycopg2.extras
import glob
import gzip
import logging as Log
import decimal
import pytz


con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./http2dns.log',format='%(asctime)s %(message)s', level=Log.INFO)
tcolumns = '(dns_id, http_id, dns_ts, http_ts, domain, ttl, dns_orig_h, dns_resp_h, http_orig_h, http_resp_h)'

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

def http2dns(httpTable, dnsTable, tname):
    records = []
    ctr = 0
    multi = 0
    global cur

    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern') #time window

    try: 
        cur.execute('SELECT DISTINCT HOST FROM %s WHERE ts>%s AND ts<%s;' % (httpTable, tw[0], tw[1]))
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(httpTable, e.pgerror))
        exit(1)
    domains = cur.fetchall()
    for row in domains:
        domain = str(row)
        print 'Processing domain %s' %(domain)
        command = '''
        INSERT INTO %s 
        SELECT %s.id, %s.id, %s.ts, %s.ts, %s.host, %s.ttls, %s.orig_h, %s.resp_h, %s.orig_h, %s.resp_h 
        FROM %s LEFT JOIN %s
        ON ((%s.host = \'%s\' OR %s.host = \'%s\') AND (%s.host = \'%s\' OR %s.query = \'%s\') AND %s.ts > %s.ts AND %s.ts < (%s.ts+%s.ttls) AND %s.ts > %s AND %s.ts < %s);
        '''
        domain_brief = ''
        if domain.split('.') == 'www':
            domain.brief = '.'.join(domain.split('.')[1:])
        else:
            domain_brief = domain

        try:
            cur.execute(command % (tname, dnsTable, httpTable, dnsTable, httpTable, httpTable, dnsTable,httpTable, httpTable, dnsTable, dnsTable, httpTable, dnsTable, httpTable, httpTable, dnsTable, dnsTable, httpTable, dnsTable, httpTable, dnsTable, dnsTable, httpTable, tw[0], httpTable, tw[1]))
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(httpTable, e.pgerror))
            exit(1)
    return
        
       
def main():
    create_new_table = '''CREATE TABLE %s
    (dns_id integer, http_id integer, dns_ts numeric, http_ts numeric, domain character varying(256), ttl double precision, dns_orig_h inet, dns_resp_h inet, http_orig_h inet, http_resp_h inet);'''
    data_to_process = '20130901'
    httpTable = 'log_' + data_to_process + '_rawts'
    dnsTable = 'dns_' + data_to_process
    tname = 'dns_and_http_' + data_to_process
    try:
        cur.execute('DROP TABLE IF EXISTS %s' % tname)
        cur.execute(create_new_table % tname)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed : %s' %(tname, e.pgerror))
        sys.exit(1)
    http2dns(httpTable, dnsTable, tname)

if __name__ == '__main__':main()

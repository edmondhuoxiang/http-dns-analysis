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

def http2dns(httpTable, dnsTable, tname):
    records = []
    ctr = 0
    global cur
    
    try:
        cur.execute('SELECT MIN(ts) from %s;' %(dnsTable))
        min1 = cur.fetchone()
        cur.execute('SELECT MIN(ts) from %s;' %(httpTable))
        min2 = cur.fetchone()
        mints = 0.0
        if min1 <  min2:
            mints = min2
        else:
            mints = min1
        cur.execute('SELECT * FROM %s where ts > %s LIMIT 5000;' %(httpTable, str(mints)[10:-3]))
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(httpTable, e.pgerror))
        exit(1)
    http_rows = cur.fetchall()
    for http_row in http_rows:
        http_ts = http_row["ts"]
        domain = http_row["host"]
        http_resp = http_row["resp_h"]
        http_orig = http_row["orig_h"]
        http_id = http_row["id"]
        domain_brief = ''
        if domain.split('.') == 'www':
            domain_brief = '.'.join(domain.split('.')[1:])
        else:
            domain_brief = domain
        
        try:
            print 'Finding #%s, domain : %s' %(http_id, domain)
            cur.execute('SELECT * FROM %s WHERE ts < %s and ts > 0 and ts > (%s - 3600) and (query = \'%s\' or query = \'%s\') order by ts desc;'% (dnsTable, http_ts, http_ts, domain, domain_brief))
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(dnsTable, e.pgerror))
            exit(1)
        dns_id = -1
        flag = False # Mark the current http record has been correlate with at least one dns record
        while True:
            dns_row = cur.fetchone() 
            if dns_row == None:
                break
            answers = dns_row["answers"]
            ttls = dns_row["ttls"]
            ttl = 0.0
            for i in range(0, len(answers)):
                if answers[i] == http_resp:
                    ttl = ttls[i]
                    break
            dns_ts = dns_row["ts"]
            if decimal.Decimal(dns_ts) > (decimal.Decimal(http_ts) - decimal.Decimal(ttl)):
                continue
            flag = True
            dns_id = dns_row["id"]
            dns_orig = dns_row["orig_h"]
            dns_resp = dns_row["resp_h"]
            try:
                print 'Get One! %d' % ctr
                (records.append((dns_id, http_id, dns_ts, http_ts, domain, ttl, dns_orig, dns_resp, http_orig, http_resp)))
                ctr = ctr + 1

            except Exception, e:
                Log.error('%s : %s : %s' %(dnsTable, httpTable, e))
                pass
            if ctr % 100 == 0:
                args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", r) for r in  records)
                try:
                    cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                    del records[:]
                    args = ''
                except pg.DatabaseError, e:
                    Log.error('%s : %s' %(tname, e.pgerror))
                    continue
                if ctr % 50000 == 0:
                    print '%d...' % ctr
        if flag == False:
            dns_id = -1
            dns_ts = -1
            ttl = 0.0
            dns_orig = '0.0.0.0'
            dns_resp = '0.0.0.0'
            try:
                print 'Found none for this http record %d' %ctr
                (records.append((dns_id, http_id, dns_ts, http_ts, domain, ttl, dns_orig, dns_resp, http_orig, http_resp)))
                ctr = ctr + 1
            except Exception, e:
                Log.error('%s : %s : %s' %(dnsTable, httpTable, e))
                pass
            if ctr % 100 == 0:
                args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", r) for r in  records)
                try:
                    cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                    del records[:]
                    args = ''
                except pg.DatabaseError, e:
                    Log.error('%s : %s' %(tname, e.pgerror))
                    continue
                if ctr % 50000 == 0:
                    print '%d...' % ctr
        flag = False
    if records:
        args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", r) for r in  records)
        try:
            cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(tname, e.pgerror))
    Log.info('%d records were logged table %s from %s and %s' %(ctr, tname, dnsTable, httpTable))
    logfile.close()
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

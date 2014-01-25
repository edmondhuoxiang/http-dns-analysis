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
import glob
import gzip
import logging as Log

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./http2dns.log',format='%(asctime)s %(message)s', level=Log.INFO)
tcolumns = '(dns_id, http_id, dns_ts, http_ts, domain, ttl, dns_orig_h, dns_resp_h, http_orig_h, http_resp_h)'

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()

except pg.DatabaseError, e:
    Log.error(e.pgerror)
    sys.exit(1)

def http2dns(httpTable, dnsTable, tname):
    records = []
    ctr = 0
    global cur
    
    try:
        cur.execute('SELECT * FROM %s LIMIT 10;' %(httpTable))
    except pg.DatabaseError, r:
        Log.error('%s : %s' %(httpTable, e.pgerror))
        exit(1)
    while True:
        rows = cur.fetchone()
        if rows == None:
            break
        for row in rows:
            print row

def main():
    data_to_process = '20130901'
    httpTable = 'log_' + data_to_process + '_rawts'
    dnsTable = 'dns_' + data_to_process
    tname = ''
    http2dns(httpTable, dnsTable, tname)


if __name__ == '__main__':main()

#!/usr/bin/env python

'''
Created on 01/27/2014

@author: Xiang Huo
'''

import sys, os, re
from operator import itemgetter
from datetime import datetime, timedelta
import time
import redis
import psycopg2 as pg
import glob
import gzip
import logging as Log

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./estimate_query_rate.log',format='%(asctime)s %(message)s', level=Log.INFO)
tcolumns = '(ts, orig_h, resp_h, host, uri, referrer, method, user_agent, status_code)'

try:
    con = pg.connect(database='tds', user='tds', host='localhost', password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    exit(1)


def getDomains(tname):
    domains = []
    global cur
    try:
        cur.execute('select distinct(query) from %s limit 10;' % tname)
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
        results.append(str(domain)[2:-3])
    return results 

def main():
    data_to_process = '20130901'
    dns_tname = 'dns_'+data_to_process
    domains = getDomains(dns_tname)
    for domain in domains:
        print domain
    print len(domains)

if __name__ == '__main__':main()

#!/usr/bin/env python

'''
Created on 03/10/2014

@author: Xiang Huo
'''

import sys, os, re
from operator import itemgetter
from datetime import datetime, timedelta
import time
import redis
import psycopg2 as pg
import psycopg2.extras
import glob
import gzip
import logging as Log
import decimal
import pytz
import pdb

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./estimate_hit_rate.log',format='%(asctime)s %(message)s', level=Log.INFO)

try:
    con = pg.connect(database='tds', user='tds', host='localhost', password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    exit(1)

def getDomains(tname, http_name):
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    domains = []
    global cur
    try:
        cur.execute('SELECT DISTINCT host from %s WHERE ts > %s AND ts < %s;' % (http_name, tw[0], tw[1]))
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    res = []
    for domain in domains:
        res.append(str(domain[2:-2]))
    return res

def getdns(domain, dns_tname):
    data_to_process = dns_tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    queries = []
    global cur
    try:
        cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND ts > %s AND ts < %s AND ttl > 0 OEDER BY ts, orig_h ASC;' % (dns_tname, domain, tw[0], tw[1]))
        queries = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(dns_tname, e))
        exit(1)
    res = []
    i = 0
    while i < len(queries)-1:
        dist = float(str(queries[i+1]['ts']))-float(str(queries[i]['ts']))
        if dist < 1 and queries[i+1]['orig_h'] == queries[i]['orig_h']:
            del(queries[i+1])
        else:
            i = i + 1
    for query in queries:
        res.append((query['ts'],query['query'],query['orig_h']))
    return res

def gethttp(domain, http_name, time):
    global cur
    row = []
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' AND ts > %s AND  ts < %s + 5 ORDER BY ts limit 1;')
        row = cur.fetchone()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_name, domain, e))
        exit(1)

    return row

def getTimeWindowOfDay(date, tz):
    timezone = pytz.timezone(tz)
    begin = date + '0:0:0'
    end = date + '23:59:59'
    date_begin = datetime.strptime(begin, "%Y%m%d %H:%M:%S")
    date_end = datetime.strptime(end, "%Y%m%d %H:%M:%S")
    date_begin_localized = timezone.localize(date_begin, is_dst=None)
    date_end_localized = timezone.localize(date_end, is_dst=None)
    return [time.mktime(date_begin_localized.timetuple()), time.mktime(date_end_localized.ti    metuple())]


def main()::
    data_to_process = '20131001'
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    dns_tname = 'dns_'+data_to_process
    http_tname = 'log_'+data_to_process+'_rawts'
    print 'tw : %s' % tw

    create_new_table = '''CREATE TABLE %s
    (domain character varying(256), dns_ts numeric, resolver inet, http_ts numeric, user inet);'''
    table = 'resolver-user'
    try:
        print 'Creating table %s' % table
        cur.execute('DROP TABLE IF EXISTS %s;' table)
        cur.execute(create_new_table % table)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (table, e.pgerror))
        sys.exit(1)


    print 'Done'
    print 'Getting all domains in %s' % http_tname
    domains = getDomains(dns_tname, http_tname)
    print 'Done'

    insert = '''INSERT INTO %s VALUES (\'%s\', %s, \'%s\', %s, \'%s\');'''
    for domain in domains:
        print 'Processing domain : %s' % domain
        
        queries = getdns(domain, dns_tname)

        for query in queries:
            ts = query[0]
            resolver = query[2]
            print ts
            request = gethttp(domain, http_tname, ts)
            if request != None:
                print request
                time = request['ts']
                user = request['orig_h']
                try:
                    cur.execute(insert % (domain, ts, resovler, time, user))
                except pg.DatabaseError, e:
                    Log.error(e.pgerror)
                    sys.exit(1)


if __name__ == '__main__':
    main()

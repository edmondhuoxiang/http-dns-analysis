#!/usr/bin/env python
'''
Created on 02/19/2014

@author: Xiang Huo
'''
import pytz
import pdb
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

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./estimate_query_rate.log',format='%(asctime)s %(message)s', level=Log.INFO)


try:
    con = pg.connect(database='tds', user='tds', host='localhost', password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    exit(1)

def getDomain(tname):
    domains = []
    global cur
    try:
        cur.execute('SELECT DISTINCT domain from %s;' % tname)
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' % (tname, e))
        exit(1)
    results = []
    for domain in domains:
        results.append(str(domain)[2:-2])
    return results

def getResolvers():
    return ['129.174.18.18','129.174.253.66','129.174.67.98','199.26.254.212']

def getAverRate(domain, tname, resolvers):
    global cur
    rates = []
    try:
        cur.execute('SELECT DISTINCT resolver, rate FROM %s WHERE domain = \'%s\';' % (tname, domain))
        rates = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' %(tname, domain , e))
        exit(1)
    total = 0.0
    print resolvers
    for r in resolvers:
        total += resolvers[r]
    res = 0.0
    if total == 0:
        return 0
    
    for rate in rates:
        if rate['resolver'] in resolvers:
            res += float(str(rate['rate']))*(float(str(resolvers[rate['resolver']]))/total)
    return res

def getDNSQuery(domain, tname):
    global cur
    queries = []
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    try:
        cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND rcode !=\'-\' AND ts > %s AND ts < %s AND ttls >= 0 ORDER BY ts asc;' %(tname, domain, tw[0], tw[1]))
        queries = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (tname, domain, e))
        exit(1)
    
    i = 0
    while i < len(queries)-1:
        dist = queries[i+1]['ts'] - queries[i]['ts']
        if dist < 1:
            del(queries[i+1])
        else:
            i = i + 1
    resolver = getResolvers()
    count = dict()
    for query in queries:
        if query['orig_h'] in count:
            tmp = count[query['orig_h']]
            tmp = tmp + 1
            count.update({query['orig_h']:tmp})
        else:
            count.update({query['orig_h']:1})
    print count
    return count

def getHTTPRequest(domain, tname):
    global cur
    count = 0
    data_to_process = tname[4:-6] 
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' AND ts > %s AND ts < %s;' % (tname, domain, tw[0], tw[1]))
        count = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (tname, domain, e))
        exit(1)

    return len(count)

def getTimeWindowOfDay(date, tz):
    timezone = pytz.timezone(tz)
    begin = date + ' 0:0:0'
    end = date + ' 23:59:59'
    date_begin = datetime.strptime(begin, "%Y%m%d %H:%M:%S")
    date_end = datetime.strptime(end, "%Y%m%d %H:%M:%S")
    date_begin_localized = timezone.localize(date_begin, is_dst=None)
    date_end_localized = timezone.localize(date_end, is_dst=None)
    return [time.mktime(date_begin_localized.timetuple()), time.mktime(date_end_localized.timetuple())]

def main():
    data_to_process = '20130902'
    dns_tname = 'dns_' + data_to_process
    http_tname = 'log_' + data_to_process + '_rawts'
    rate_tname = 'estimate_rate_20131001_v2'
    res_tname = 'result_table'
    create_new_table = '''CREATE TABLE %s
    (domain character varying (256), rate numeric, dns_query int, estimated_request numeric, actual_request int);'''

    try:
        cur.execute('DROP TABLE IF EXISTS %s' % res_tname)
        cur.execute(create_new_table % res_tname)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed : %s' % (res_tname, e.pgerror))
        exit(1)

    domains = getDomain(rate_tname)
    for domain in domains:
        print 'Processing domain : %s' % domain
        count = getDNSQuery(domain, dns_tname)
        rate = getAverRate(domain, rate_tname, count)
        actual_req = getHTTPRequest(domain, http_tname)
        dns_query = 0
        for c in count:
            dns_query += count[c]
        estimated = dns_query * rate
        try:
            cur.execute('INSERT INTO %s VALUES (\'%s\', %s, %s, %s, %s);' % (res_tname, domain, rate, dns_query, estimated, actual_req))
        except pg.DatabaseError, e:
            Log.error(e.pgerror)
            sys.exit(1)

if __name__ == '__main__':main()

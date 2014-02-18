#!/usr/bin/env python

'''
Created on 02/17/2014

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

def getDomains(tname):
    domains = []
    global cur
    try:
        cur.execute('SELECT DISTINCT query FROM %s WHERE rcode != \'-\' AND ttls >= 0 GROUP BY query HAVING COUNT(*) > 10 LIMIT 500;' % tname)
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
        results.append(str(domain)[2:-2])
    return results

def getResolversForDomain(domain, tname):
    resolvers = []
    global cur
    try:
        cur.execute('SELECT DISTINCT orig_h FROM %s WHERE query = \'%s\';' %(tname, domain))
        resolvers = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for resolver in resolvers:
        results.append(str(resolver)[2:-2])
    return results


def getNumOfCircle(domain, resolver, tname):
    num = 0
    global cur
    try:
        cur.execute('SELECT COUNT(*) FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' AND rcode != \'-\' AND ttls >= 0;' %(tname, domain, resolver))
        num = fetchone()
    except pg.DatabaseError, e:
        Log.error('%s : %s' % (tname, e))
        exit(1)
    print 'The number of circles is: %s\n' % num
    return num

def getAllCircles(domain, resolver, dns_tname, http_tname):
    dns_queries = []
    http_requests = []
    global cur
    try:
        cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' ORDER BY ts ASC;' %(dns_tname, domain, resolver))
        dns_queries = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s : %s' %(dns_tname, domain, resolver, e))
        exit(1)
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' ORDER BY ts ASC;' %(http_tname, domain))
        http_requests = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_tname, domain, e))
        exit(1)
    circles = []
    index = 0
    for query in dns_queries:
        count = 0
        while index < len(http_requests):
            request = http_requests[index]
            index = index + 1
            if request['ts'] > query['ts'] and request['ts'] <= (query['ts']+query['ttls']):
                count = count + 1
            else:
                circles.append((query['ts'], query['ts']+query['ttls'], count))
                break
    return circles

def getAllCircles_v2(domain, resolvers, dns_tname, http_tname):
    dns_queries = []
    http_requests = []
    global cur
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' ORDER BY ts ASC;' % (http_name, domain))
        http_requests = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_tname, domain, e))
        exit(1)

    for resolver in resolvers:
        try:
            cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' ORDER BY ts ASC;' % (dns_tname, domain, resolver))
            tmp = cur.fetchall()
        except pg.DatabaseError, e:
            Log.error('%s : %s : %s : %s' % (dns_tname, domain, resolver, e))
            exit(1)
        dns_queries.append(tmp)
    
    circles = []
    index = []
    count = []
    for i in range(0, len(resolvers)):
        circles.append([])
        index.append(0)
        count.append(0.0)
    
    for request in http_requests:
        tmp_index = []
        for i in range(0, len(resolvers)):
            if request['ts'] > http_requests[i][index[i]]['ts'] and request['ts'] < (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                tmp_index.append(1);
            else:
                tmp_index.append(0);
                if request['ts'] > (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                    circles[i].append(http_requests[i][index[i]]['ts'], htpp_requets[i][index[i]]['ts']+http_request[i][index[i]]['ttls'], count[i] )
                    count[i] = 0.0
        for i in range(0, tmp_index):
            count[i] = count[i] + tmp_index[i]/sum(tmp_index)

    return circles

def getMaxhits(circles):
    maxNum = 0
    for circle in circles:
        if circle[2] > maxNum:
            maxNum = circle[2]
    return maxNum

def getRateOfHits(circles):
    maxhits = getMaxhits(circles)
    length = len(circles)
    rate = 0.0
    for n in range(0, maxhits+1):
        count = 0
        for circle in circles:
            if circle[2] == n:
                count = count + 1
        rate = rate + n*(float(count)/float(length))
    return rate

def getAllRates(domain, dns_tname, http_tname):
    print 'Getting all resolvers for %s' % domain
    resolvers = getResolversForDomain(domain, dns_tname)
    print 'Done'
    res = []
    for resolver in resolvers:
        print 'Getting circles for resolver: %s' % resolver
        circles = getAllCircles(domain, resolver, dns_tname, http_tname)
        print 'Calculating rate' 
        rate = getRateOfHits(circles)
        print 'Done'
        res.append(domain, resolver, rate)
    #circles = getAllCircles_v2(domain, resolvers, dns_tname, http_tname)
    #for i in range(0,resolvers):
    #    rate = getRateOfHits(circles[i])
    #    res.append(domain, resolvers[i], rate)
    return res


def main():
    data_to_process = '20131001'
    dns_tname = 'dns_' + data_to_process
    http_tname = 'log_' + data_to_process + '_rawts'
    create_new_table = '''CREATE TABLE %s
    (domain character varying(256), resolver inet, rate numeric);'''
    estimate_table = 'estimate_rate_' + data_to_process
    try: 
        cur.execute('DROP TABLE IF EXISTS %s;' % estimate_table)
        cur.execute(create_new_table % estimate_table)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (estimate_table, e.pgerror))
        sys.exit(1)

    print 'Getting all domains in %s' % dns_tname
    domains = getDomains(dns_tname)
    print 'Done'
    for domain in domains:
        print 'Processing domain : %s\n' % domain
        res = getAllRates(domain, dns_tname, http_tname)
        for entry in res:
            print '\tResolver : %s\tRate : %f\n' %(entry[1], entry[2])
            insert = '''INSERT INTO %s VALUES
            (%s, %s, %f);'''
            try:
                cur.execute(insert %(res[0], res[1], res[2]))
            except pg.DatabaseError, e:
                Log.error(e.pgerror)
                sys.exit(1)

if __name__ == '__main__':
    main()

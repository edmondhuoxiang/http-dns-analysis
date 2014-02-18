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

def getDomains(tname):
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    domains = []
    global cur
    try:
        cur.execute('SELECT DISTINCT query FROM %s WHERE rcode != \'-\' AND ttls >= 0 AND ts > %s AND ts < %s GROUP BY query HAVING COUNT(*) > 10 LIMIT 500;' % (tname, tw[0], tw[1]))
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
        results.append(str(domain)[2:-2])
    return results

def getResolversForDomain(domain, tname, flag):
    print flag
    if flag == True:
        print 'Static model to get resolvers'
        return ['129.174.18.18', '129.174.253.66', '129.174.67.98','199.26.254.212']
    resolvers = []
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    global cur
    try:
        cur.execute('SELECT DISTINCT orig_h FROM %s WHERE query = \'%s\' AND ts > %s AND ts < %s;' %(tname, domain, tw[0], tw[1]))
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
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    global cur
    try:
        cur.execute('SELECT COUNT(*) FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' AND rcode != \'-\' AND ttls >= 0 AND ts > %s AND ts < %s;' %(tname, domain, resolver, tw[0], tw[1]))
        num = fetchone()
    except pg.DatabaseError, e:
        Log.error('%s : %s' % (tname, e))
        exit(1)
    print 'The number of circles is: %s' % num
    return num

def getAllCircles(domain, resolver, dns_tname, http_tname):
    dns_queries = []
    http_requests = []
    data_to_process = dns_tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    global cur
    try:
        cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' %(dns_tname, domain, resolver, tw[0], tw[1]))
        dns_queries = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s : %s' %(dns_tname, domain, resolver, e))
        exit(1)
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' %(http_tname, domain, tw[0], tw[1]))
        http_requests = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_tname, domain, e))
        exit(1)
    circles = []
    index = 0
    i = 0
    while i < len(dns_queries)-1:
        dist = float(str(dns_queries[i+1]['ts'])) - float(str(dns_queries[i]['ts']))
        print dist
        if dist < 1:
            del(dns_queries[i+1])
        else:
            i = i+1
    #pdb.set_trace()
    for query in dns_queries:
        count = 0
        while index < len(http_requests):
            request = http_requests[index]
            index = index + 1
            ts_0 = float(str(request['ts']))
            ts_1 = float(str(query['ts']))
            ttl = float(str(query['ttls']))
            if ts_0 > ts_1 and ts_0 <= (ts_1+ttl):
            #if request['ts'] > query['ts'] and request['ts'] <= (query['ts']+query['ttls']):
                count = count + 1
            elif ts_0 <= ts_1:
                continue
            else:
                index = index -1
                #circles.append((query['ts'], query['ts']+query['ttls'], count)
                #circles.append((ts_1, ts_1+ttl, count))
                break
        circles.append((ts_1, ts_1+ttl, count))
    #pdb.set_trace()
    print 'circles: %s' % circles
    
    return circles

def getAllCircles_v2(domain, resolvers, dns_tname, http_tname):
    dns_queries = []
    http_requests = []
    data_to_process = dns_tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    global cur
    try:
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' % (http_name, domain, tw[0], tw[1]))
        http_requests = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_tname, domain, e))
        exit(1)

    for resolver in resolvers:
        try:
            cur.execute('SELECT * FROM %s WHERE query = \'%s\' AND orig_h = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' % (dns_tname, domain, resolver, tw[0], tw[1]))
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
            ts_0 = float(str(request['ts']))
            ts_1 = float(str(dns_queries[i][index[i]]['ts']))
            ttl = float(str(dns_queries[i][index[i]]['ttls']))
            if ts_0 > ts_1 and ts_0 < (ts_1+ttl):
            #if request['ts'] > http_requests[i][index[i]]['ts'] and request['ts'] < (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                if ts_0 - ts_1 < 1.0:
                    tmp_index = []
                    for j in range(0, len(resolvers)):
                        if j == i:
                            tmp_index.append(1)
                        else:
                            tmp_index.append(0)
                    break
                tmp_index.append(1);
            else:
                tmp_index.append(0);
                if ts_0 > (ts_1+ttl):
                #if request['ts'] > (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                    circles[i].append(ts_1, ts_1+ttl, count[i])
                    #circles[i].append(http_requests[i][index[i]]['ts'], htpp_requets[i][index[i]]['ts']+http_request[i][index[i]]['ttls'], count[i] )
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
        if length != 0:
            rate = rate + n*(float(count)/float(length))
        else:
            rate = rate + 0
    return rate

def getAllRates(domain, dns_tname, http_tname):
    print 'Getting all resolvers for %s' % domain
    resolvers = getResolversForDomain(domain, dns_tname, True)
    print 'Done'
    res = []
    for resolver in resolvers:
        print 'Getting circles for resolver: %s' % resolver
        circles = getAllCircles(domain, resolver, dns_tname, http_tname)
        print 'Calculating rate' 
        rate = getRateOfHits(circles)
        print 'Done'
        res.append((domain, resolver, rate))
    #print 'Getting circles for all resolvers'
    #circles = getAllCircles_v2(domain, resolvers, dns_tname, http_tname)
    #print 'Done'
    #for i in range(0,resolvers):
    #    print 'Calculating rate'
    #    rate = getRateOfHits(circles[i])
    #    print 'Done'
    #    res.append((domain, resolvers[i], rate))
    return res

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
    data_to_process = '20131001'
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    dns_tname = 'dns_' + data_to_process
    http_tname = 'log_' + data_to_process + '_rawts'
    print 'tw : %s' % tw
    create_new_table = '''CREATE TABLE %s
    (domain character varying(256), resolver inet, rate numeric);'''
    estimate_table = 'estimate_rate_' + data_to_process
    try: 
        print 'Creating table %s' % estimate_table
        cur.execute('DROP TABLE IF EXISTS %s;' % estimate_table)
        cur.execute(create_new_table % estimate_table)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (estimate_table, e.pgerror))
        sys.exit(1)
    print 'Done'
    print 'Getting all domains in %s' % dns_tname
    domains = getDomains(dns_tname)
    print 'Done'
    for domain in domains:
        print 'Processing domain : %s' % domain
        res = getAllRates(domain, dns_tname, http_tname)
        for entry in res:
            print '\tResolver : %s\tRate : %f' %(entry[1], entry[2])
            insert = '''INSERT INTO %s VALUES
            (\'%s\',\'%s\', %f);'''
            try:
                cur.execute(insert %(estimate_table, entry[0], entry[1], entry[2]))
            except pg.DatabaseError, e:
                Log.error(e.pgerror)
                sys.exit(1)

if __name__ == '__main__':
    main()

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

def getDomains(tname, http_tname):
    data_to_process = tname[-8:]
    tw = getTimeWindowOfDay(data_to_process, 'US/Eastern')
    domains = []
    global cur
    try:
        cur.execute('SELECT DISTINCT host FROM %s WHERE ts > %s AND ts < %s GROUP BY host HAVING COUNT(*) > 10 LIMIT 600;' % (http_tname, tw[0], tw[1]))
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
        cur.execute('SELECT * FROM %s WHERE ttls > 0 AND rcode != \'-\' AND query = \'%s\' AND orig_h = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' %(dns_tname, domain, resolver, tw[0], tw[1]))
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
        ts_1 = float(str(query['ts']))
        ttl = float(str(query['ttls']))
        while index < len(http_requests):
            request = http_requests[index]
            index = index + 1
            ts_0 = float(str(request['ts']))
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
        cur.execute('SELECT * FROM %s WHERE host = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' % (http_tname, domain, tw[0], tw[1]))
        http_requests = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s' % (http_tname, domain, e))
        exit(1)
    
    circles = []
    index = []
    count = []
    tmp_index = []
    for i in range(0, len(resolvers)):
        circles.append([])
        index.append(0)
        count.append(0.0)
        dns_queries.append([])
        tmp_index.append(0)
    try: 
        print 'Selecting records from db...'
        cur.execute('SELECT * FROM %s WHERE ttls > 0 AND rcode != \'-\' AND query = \'%s\' AND ts > %s AND ts < %s ORDER BY ts ASC;' % (dns_tname, domain, tw[0], tw[1]))
        tmp = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s : %s : %s' % (dns_tname, domain, resolver, e))
        exit(1)

    print 'filtering into differenct queue'
    for entry in tmp:
        for i in range(0, len(resolvers)):
            #print str(entry['orig_h'])
            if resolvers[i] == str(entry['orig_h']):
                dns_queries[i].append(entry)

    i = 0

    #print dns_queries
    #pdb.set_trace()
    print 'Deleting duplicate dns quries'
    while i < len(dns_queries):
        j = 0
        while j < (len(dns_queries[i]) - 1):
            #print dns_queries
            dist = float(str(dns_queries[i][j+1][1])) - float(str(dns_queries[i][j][1]))
            if dist < 1:
                del(dns_queries[i][j+1])
            else:
                j = j + 1
        i = i + 1
   
    #pdb.set_trace()
    print 'Making pairs of dns queries and http requests'
    for request in http_requests:
        for k in  range(0, len(tmp_index)):
            tmp_index[k] = 0
        ts_0 = float(str(request['ts']))
        for i in range(0, len(resolvers)):
            if index[i] < len(dns_queries[i]):
                ts_1 = float(str(dns_queries[i][index[i]]['ts']))
                ttl = float(str(dns_queries[i][index[i]]['ttls']))
                if ts_0 > ts_1 and ts_0 < (ts_1+ttl):
                #if request['ts'] > http_requests[i][index[i]]['ts'] and request['ts'] < (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                    if ts_0 - ts_1 < 1.0:
                        for j in range(0, len(resolvers)):
                            if j == i:
                                tmp_index[j] = 1
                            else:
                                tmp_index[j] = 0
                        break
                    tmp_index[i] = 1
                else:
                    tmp_index[i] = 0;
                    if ts_0 > (ts_1+ttl):
                    #if request['ts'] > (http_requests[i][index[i]]['ts']+http_requests[i][index[i]]['ttls']):
                        circles[i].append((ts_1, ts_1+ttl, count[i]))
                        #circles[i].append(http_requests[i][index[i]]['ts'], htpp_requets[i][index[i]]['ts']+http_request[i][index[i]]['ttls'], count[i] )
                        count[i] = 0.0
                        index[i] = index[i] + 1
            else:
                tmp_index[i] = 0
        tmp_sum = 0.0
        for k in range(0, len(tmp_index)):
            tmp_sum = tmp_sum + tmp_index[k]
        for i in range(0, len(tmp_index)):
            if tmp_sum != 0:
                count[i] = count[i] + float(tmp_index[i])/float(tmp_sum)
    print count
    for i in range(0, len(resolvers)):
        if index[i] < len(dns_queries[i]):
            ts_1 = float(str(dns_queries[i][index[i]]['ts']))
            ttl = float(str(dns_queries[i][index[i]]['ttls']))
            circles[i].append((ts_1, ts_1+ttl, count[i]))
        else:
            if len(dns_queries[i]) > 0:
                ts_1 = float(str(dns_queries[i][-1]['ts']))
                ttl = float(str(dns_queries[i][-1]['ttls']))
                circles[i].append((ts_1, ts_1+ttl, count[i]))

    print circles
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
    for n in range(0, int(maxhits)+1):
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
    #for resolver in resolvers:
    #    print 'Getting circles for resolver: %s' % resolver
    #    circles = getAllCircles(domain, resolver, dns_tname, http_tname)
    ##    print 'Calculating rate' 
    #    rate = getRateOfHits(circles)
    #    print 'Done'
    #    res.append((domain, resolver, rate))
    print 'Getting circles for all resolvers'
    circles = getAllCircles_v2(domain, resolvers, dns_tname, http_tname)
    print 'Done'
    for i in range(0,len(resolvers)):
        print 'Calculating rate'
        rate = getRateOfHits(circles[i])
        print 'Done'
        res.append((domain, resolvers[i], rate))
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
    estimate_table = 'estimate_rate_' + data_to_process + '_v2'
    try: 
        print 'Creating table %s' % estimate_table
        cur.execute('DROP TABLE IF EXISTS %s;' % estimate_table)
        cur.execute(create_new_table % estimate_table)
    except pg.DatabaseError, e:
        Log.error('Creating new table %s failed: %s' % (estimate_table, e.pgerror))
        sys.exit(1)
    print 'Done'
    print 'Getting all domains in %s' % dns_tname
    domains = getDomains(dns_tname, http_tname)
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

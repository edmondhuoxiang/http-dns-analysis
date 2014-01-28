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
import psycopg2.extras
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
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    exit(1)


def getDomains(tname):
    domains = []
    global cur
    try:
        #cur.execute('select distinct(query) from %s limit 10;' % tname)
        cur.execute('select distinct query from %s group by query having count(*) > 30 order by query desc limit 10;' % tname)
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
        print str(domain)
        results.append(str(domain)[2:-2])
    return results 

MIN_SERIES_SIZE = 10

class Record:
    def __init__(self, tname, query):
        global cur
        self.resolvers = []
        try:
            cur.execute('SELECT DISTINCT orig_h FROM %s WHERE query = \'%s\';' %(tname, query));
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(tname, e))
            exit(1)
        while True:
            resolver = cur.fetchone()
            if resolver == None:
                break
            print 'Resolver: %s' %(str(resolver)[2:-2])
            self.resolvers.append(str(resolver)[2:-2])
        self.series = []
        for i in range(0, len(self.resolvers)):
            self.series.append([])


        try:
            cur.execute('SELECT * FROM %s where query = \'%s\';' %(tname, query))
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(tname, e))
            exit(1)
        self.domain = query
        self.max_ttl = 0

        while True:
            record = cur.fetchone()
            if record == None:
                break
            ts = float(str(record["ts"]))
            ttls = record["ttls"]
            orig = record["orig_h"]
            for i in range(0, len(self.resolvers)):
                if orig == self.resolvers[i]:
                    ttl = 0.0
                    for j in range(0, len(ttls)):
                        ttl = ttl + ttls[j]
                    ttl = float(ttl)/len(ttls)
                    #if ttl > 0:
                    self.series[i].append([ts, ttl])
                    if ttl > self.max_ttl:
                        self.max_ttl = ttl
        print 'SERIES'
        print self.series
        self.series.sort(key=itemgetter(0))

    def estimate_rate(self):
        result = []
        for j in range(0, len(self.resolvers)):
            if len(self.series[j]) < MIN_SERIES_SIZE:
                print 'Length of series is %d less than MIN_SERIES_SIZE' % len(self.series[j])
                result.append(-1)
                continue

            estimate = 0
            count = 0
            del_num = 0
            index = 0
            while index < (len(self.series[j])-1-del_num):
                print self.series[j][index][0]
                num1 = int(self.series[j][index][0])
                print self.series[j][index+1][0]
                num2 = int(self.series[j][index+1][0])
                ttl = self.series[j][index][1]
                if ttl < 0:
                    del self.series[j][index]
                    del_num = del_num + 1
                    continue
                if num1 == num2:
                    del self.series[j][index+1]
                    del_num = del_num + 1
                    continue 
                index = index +1
            flag = False
            print '***'
            for i in range(0, len(self.series[j])-1):
                print self.series[j][i]
            for i in range(0, len(self.series[j])-1):
                ts_1 = int(self.series[j][i+1][0])
                ts_0 = int(self.series[j][i][0])
                ttl = int(self.series[j][i][1])

                delta = ts_1 - (ts_0 + ttl)

                if delta < self.max_ttl:
                    delta_x = ts_1 - ts_0
                    if delta_x >= 0:
                        delta = delta_x
                    else:
                        print "Warning! Queries arriving faster than TTL should allow"
                        print self.domain, ts_1, ts_0, ts_1-ts_0, self.max_ttl
                        result.append(-1)
                        flag = True
                        break
                estimate += delta
                count += 1
            if flag:
                continue
            else:
                result.append(count/float(estimate))
        return result

def estimate_day(tname):
    print 'Estimating data of a data ....'
    domains = getDomains(tname)
    print 'Get all distinct domain ....'
    domain_rates = {}
    for domain in domains:
        print 'Processing domain : %s' % domain
        r = Record(tname, domain)
        domain = r.domain
        resolvers = r.resolvers
        query_rate = r.estimate_rate()

        if domain not in domain_rates:
            domain_rates[domain] = []
        for i in range(0, len(query_rate)):
            print 'Roselver : %s\t Rate : %f\n' %(resolvers[i], query_rate[i])
            domain_rates[domain].append((resolvers[i], query_rate[i]))


    rates = domain_rates.items()
    return rates


def main():
    data_to_process = '20130901'
    dns_tname = 'dns_'+data_to_process
    rates = estimate_day(dns_tname)
    output = open('query_rate_by_day.data', 'w')
    for domain, query_rate in rates:
        output.write("%s\t%s\n" %(domain, query_rate))
    output.close()

if __name__ == '__main__':
    main()

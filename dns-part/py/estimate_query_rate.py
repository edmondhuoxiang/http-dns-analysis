#!/usr/bin/env python

'''
Created on 01/27/2014

@author: Xiang Huo
'''
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
tcolumns = '(ts, orig_h, resp_h, host, uri, referrer, method, user_agent, status_code)'

output = open('query_rate_by_day.data', 'w')

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
        cur.execute('select distinct query from %s WHERE rcode != \'-\' AND ttls >= 0 group by query having count(*) > 10 limit 500;' % tname)
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
        results.append(str(domain)[2:-2])
    return results 

def getDomainsFromDB(tname, dev):
    domains = []
    global cur
    try:
        cur.execute('SELECT count(*) from %s;' % tname)
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    count = cur.fetchone()
    min_index = 0
    max_index = 0
    if count < (2*dev):
        min_index = 0
        max_index = count
    else:
        min_index = 10000-dev
        max_index = 10000+dev
    try:
        cur.execute('SELECT domain from %s WHERE id > %d and id < %d;' % (tname, min_index, max_index))
        domains = cur.fetchall()
    except pg.DatabaseError, e:
        Log.error('%s : %s' %(tname, e))
        exit(1)
    results = []
    for domain in domains:
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
            cur.execute('SELECT * FROM %s where query = \'%s\' AND rcode != \'-\' AND ttls >= 0 ORDER BY orig_h, ts, id;' %(tname, query))
        except pg.DatabaseError, e:
            Log.error('%s : %s' %(tname, e))
            exit(1)
        self.domain = query
        self.max_ttl = 0

        ts = 0.0
        num = 0
        orig = ''
	ttl = 0.0
        while True:
            #pdb.set_trace()
            record = cur.fetchone()
            if record == None:
                break
            if ts == float(str(record["ts"])):
                ttl = ttl + record["ttls"]
                num = num + 1
		ts = float(str(record["ts"]))
            else:
                if orig != '':
                    ttl = float(ttl)/num
                    for i in range(0, len(self.resolvers)):
                        if orig == self.resolvers[i]:
                            self.series[i].append([ts,ttl])
                ts = float(str(record["ts"]))
                ttl = record["ttls"]
                orig = record["orig_h"]
                num = 1
            if record["ttls"] > self.max_ttl:
                self.max_ttl = record["ttls"]
	ttl = float(ttl)/num
	for i in range(0, len(self.resolvers)):
	    if orig == self.resolvers[i]:
	        self.series[i].append([ts, ttl])
        for i in range(0, len(self.series)):
            self.series[i].sort(key=itemgetter(0))
    
    def estimate_rate(self):
        result = []
        for j in range(0, len(self.resolvers)):
            if len(self.series[j]) < MIN_SERIES_SIZE:
                print 'Length of series is %d less than MIN_SERIES_SIZE' % len(self.series[j])
                result.append(-1)
                continue

	    #print self.series[j]
            estimate_1 = 0
            count_1 = 0
            estimate_2 = 0
            count_2 = 0
            index = 0
            while index < (len(self.series[j])-1):
                num1 = int(self.series[j][index][0])
                num2 = int(self.series[j][index+1][0])
                if num1 == num2:
                    del self.series[j][index+1]
                else:
                    index = index +1
	    #pdb.set_trace()
            flag = False
            for i in range(0, len(self.series[j])-1):
                ts_1 = int(self.series[j][i+1][0])
                ts_0 = int(self.series[j][i][0])
                ttl = int(self.series[j][i][1])

                delta_1 = ts_1 - (ts_0 + ttl)
                delta_2 = ts_1 - ts_0

                if delta_1 < 0 or delta_2 < 0:
                    print "Warning! Queries arriving faster than TTL should allow"
                    print self.domain, ts_1, ts_0, ts_1-ts_0, self.max_ttl
                    result.append(-1)
                    flag = True
                    Log.error("Warning! Queries arriving faster than TTL should allow")
                    Log.error(self.domain)
                    break
                estimate_1 += delta_1
                estimate_2 += delta_2
                count_1 += 1
                count_2 += 2
            if flag:
                continue
            else:
                res = (0.0,0.0)
                if estimate_1 == 0:
                    res[0] = -2
                else:
                    res[0] = float(count_1)/float(estimate_1)
                if estimate_2 == 0:
                    res[1] == -2
                else:
                    res[1] = float(count_2)/float(estimate_2)
                result.append(res)
        return result

def estimate_day(tname, estimate_tname):
    global output
    print 'Estimating data of a date ....'
    #domains = getDomains(tname)
    #domains.append('google.com')
    #domains.append('facebook.com')
    #domains.append('youtube.com')
    domains = getDomainsFromDB(estimate_tname, 100)
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
        flag = False
        for i in range(0, len(query_rate)):
            print 'Roselver : %s\t Rate : %f\n' %(resolvers[i], query_rate[i])
            domain_rates[domain].append((resolvers[i], query_rate[i]))
        string = ''
        global_rate_1 = 0.0
        global_rate_2 = 0.0
        for i in range(0, len(query_rate)):
            if (query_rate[i][0] > 0 and query_rate[i][1]> 0 )and flag == False:
                print 'writing to file...'
                string += domain
                flag = True
            if query_rate[i][0] > 0 and query_rate[i][1] > 0:
                string += '\t'+resolvers[i]+','+str(query_rate[i][0])+','+str(query_rate[i][1])
                global_rate_1 = global_rate_1 + query_rate[i][0]
                global_rate_2 = global_rate_2 + query_rate[i][1]
	string += '\n'
        if flag == True:
            output.write(string)

            try:
                #print 'SELECT count from %s where domain =\'%s\';)'%(estimate_tname, domain)
                cur.execute('SELECT count from %s where domain =\'%s\';'%(estimate_tname, domain))
            except pg.DatabaseError, e:
                Log.error('%s : %s : %s' %(estimate_tname, domain, e.pgerror))
            count = int(str(cur.fetchone())[1:-1])
	    #print count
            estimate_vol = global_rate * 24 * 3600
            distance = abs(estimate_vol - count)

            try:
                cur.execute('UPDATE %s SET rate_1 = %f, rate_2 = %f, estimated_vol = %f, distance = %f where domain = \'%s\';' %(estimate_tname, global_rate_1, global_rate_2, estimate_vol, distance, domain))
            except pg.DatabaseError, e:
                Log.error('%s : %s' %(estimate_tname, e))
    rates = domain_rates.items()
    return rates


def main():
    data_to_process = '20131001'
    dns_tname = 'dns_'+data_to_process
    estimate_tname = 'estimate_' + data_to_process
    #dns_tname = 'dns_test'
    output.write("#The estimated date is : %s\n" % data_to_process)
    rates = estimate_day(dns_tname, estimate_tname)
    #output = open('query_rate_by_day.data', 'w')
    #for domain, query_rate in rates:
    #    if query_rate > 0 :
    #        output.write("%s\t%s\n" %(domain, query_rate))
    output.close()

if __name__ == '__main__':
    main()

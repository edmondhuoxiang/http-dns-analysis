#!/usr/bin/env python

'''
Created on 01/15/2014

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
Log.basicConfig(filename='./dns2db.log',format='%(asctime)s %(message)s', level=Log.INFO)
tcolumns = '(ts, orig_h, resp_h, query, rcode, answers, ttls)'

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()

except pg.DatabaseError, e:
    Log.error(e.pgerror)
    sys.exit(1)

def calculateDate(orig_str, offset):
    orig_date = datetime.strptime(orig_str, '%Y%m%d')
    d = timedelta(days = offset)
    new_date = orig_date + d
    return new_date.strftime('%Y%m%d')

def asDigitBinary(source):
    return '{0:08b}'.format(source)

def isBeginWithPrefix(ip, prefix):
    num = int(prefix.split('/')[1])
    prefixStrArr = prefix.split('/')[0].split('.')
    ipStrArr = ip.split('.')
    i = num
    j = 0
    while(i>0):
        str_1 = asDigitBinary(int(ipStrArr[j]))
        str_2 = asDigitBinary(int(prefixStrArr[j]))
        count=0
        if i<8:
            count = i
        else:
            count = 8
        for k in range(0, count):
            if(str_1[k] != str_2[k]):
                return False
        j = j+1
        i = i-8
    return True

def isInPrefix(ip, prefixArr):
    for prefix in prefixArr:
        res = isBeginWithPrefix(ip, prefix)
        if res == True:
            return True
    return False

def log2db(lfile, tname):
    records = []
    ctr = 0
    global cur
    try:
        logfile = gzip.open(lfile, 'r')
    except IOError, e:
        Log.error('%s : %s' %(lfile,e))
        return
    while True:
        try:
            line = logfile.readline()
        except IOError, e:
            Log.error('%s : %s' %(lfile,e))
            logfile.close()
            return
        if not line:
            if records:
                args = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s)', r) for r in records)
                try:
                    cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                except pg.DatabaseError, e:
                    Log.error('%s : %s' %(lfile, e.pgerror))
                Log.info('%d records were logged to db from %s' %(ctr, lfile))
                logfile.close()
                return
        fields = line.split('\t')
        if fields[0] == '#fields':
            fieldnames = fields[1:]
            fieldnames[-1] = fieldnames[-1][:-1]
            continue
        if fields[0].find('#') == 0:
            continue
        keyval = dict(zip(fieldnames, fields))
        prefixArr = ["129.174.0.0/16", "129.174.0.0/17", "129.174.128.0/17", "129.174.130.0/23", "129.174.176.0/20", "192.5.215.0/24", "199.26.254.0/24"]
        if isInPrefix(keyval['id.orig_h'], prefixArr)==False:
            continue
        if isInPrefix(keyval['id.resp_h'], prefixArr)==True:
            continue
        try:
            answerArr = keyval['answers'].split(',')
            ttlArr = keyval['TTLs'].split(',')
            if len(answerArr) != len(ttlArr):
                print 'Error on record with timestamp as %s' %keyval['ts']
                continue
            for index in range(0, len(answerArr)):
                answer = answerArr[index]
                ttl = ttlArr[index]
                if ttl == '-\n':
                   ttl = '-1'
                (records.append((keyval['ts'], keyval['id.orig_h'], keyval['id.resp_h'], keyval['query'][:256].rstrip(),keyval['rcode'].rstrip(), answer, ttl)))
                ctr = ctr + 1
            '''
            answerArr = keyval['answers'].split(',')
            answers = "{"
            for answer in answerArr:
                answers = answers + "\"" + answer + "\","
            answers = answers[:-1] + "}"
            ttls = "{"
            if keyval['TTLs'] == "-\n":
                ttls = ttls + "-1}"
            else:
                ttls = ttls + keyval['TTLs'][:-1] + "}"
            (records.append((keyval['ts'], keyval['id.orig_h'], keyval['id.resp_h'], keyval['query'][:256].rstrip(),keyval['rcode'].rstrip(), answers.rstrip(), ttls.rstrip())))
            '''
        except Exception, e:
            Log.error('%s : %s' %(lfile,e))
            print fieldnames
            pass
        #ctr = ctr + 1
        if ctr % 10000 == 0:
            if ctr % 50000 == 0:
                print '%d......' % ctr
            args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)",r) for r in records)
            try:
                cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                del records[:]
                args = ''
            except pg.DatabaseError, e:
                Log.error('%s : %s' %(lfile, e.pgerror))
                continue


def main():
    create_new_table = '''CREATE TABLE %s
    (id serial primary key NOT NULL, ts numeric, orig_h inet, resp_h inet, query character varying(256), rcode character varying(2), answers text, ttls double precision);'''
    data_to_process = '20130901'
    logdir = '/raid/pdns_bro/%s' % data_to_process
    if os.path.exists(logdir):
        tname = 'dns_' + data_to_process 
        try:
            cur.execute('DROP TABLE IF EXSITS %s;' % tname)
            cur.execute(create_new_table % tname)
        except pg.DatabaseError, e:
            Log.error('Creating new table %s failed : %s' % (tname, e.pgerror))
            sys.exit(1)
        logfiles = glob.glob(logdir + '/*.log.gz')
        
        if logfiles:
            for logfile in logfiles:
                print 'processing %s' %logfile
                log2db(logfile, tname)
        candidate_logdir = 'raid/pdns_bro/%s' %(calculateDate(data_to_process, 1))
        logfiles = glob.glob(candidate_logdir + '/*.log.gz')
        if logfiles:
            for logfile in logfiles:
                print 'processing %s' %logfile
                log2db(logfile, tname)
        candidate_logdir = 'raid/pdns_bro/%s' %(calculateDate(data_to_process, -1))
        logfiles = glob.glob(candidate_logdir + '/*.log.gz')
        if logfiles:
            for logfile in logfiles:
                print 'processing %s' %logfile
                log2db(logfile, tname)

    else:
        Log.info('No log directory was found %s' % data_to_process)
        sys.exit(0)
if __name__ == '__main__':main()

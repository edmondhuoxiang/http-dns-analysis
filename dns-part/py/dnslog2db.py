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
            continue
        if fields[0].find('#') == 0:
            continue
        keyval = dict(zip(fieldnames, fields))
        try:
            answersArr = keyval['answers'].split(',')
            answers = "{"
            for answer in answerArr:
                answers = answers + "\"" + answer + "\","
            answers = answers[:-1] + "}"
            ttls = "{"
            if keyval['ttls'] == "-\n":
                ttls = ttls + "-1}"
            else:
                ttls = ttls + keyval['ttls'] + "}"

            (records.append((keyval['ts'], keyval['id.orig_h'], keyval['id.resp_h'], keyval['query'][:256].rstip(),keyval['rcode'].rstrip(), answers.rstrip(), ttls.rstrip())))
        except Exception, e:
            Log.error('%s : %s' %(lfile,e))
            pass
        if ctr % 10000 == 0:
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
    (ts numeric, orig_h inet, resp_h inet, query character varying(256), rcode character varying(2), answers text[], ttls double precision[]);'''
    data_to_process = '20130901'
    logdir = '/raid/pdns_bro/%s' % data_to_process
    if os.path/exists(logdir):
        tname = 'dns_' + data_to_process + '_rawts_2'
        try:
            cur.execute(create_new_table % tname)
        except pg.DatabaseError, e:
            Log.error('Creating new table %s failed : %s' % (tname, e.pgerror))
            sys.exit(1)
        logfiles = glob.glob(logdir + '/*.log.gz')
        if logfiles:
            for logfile in logfiles:
                log2db(logfile, tname)

    else:
        Log.info('No log directory was found %s' % data_to_process)
        sys.exit(0)
if __name__ == '__main__':main()

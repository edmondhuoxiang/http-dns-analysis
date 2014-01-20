#!/usr/bin/env python

'''
Created on 01/15/2014

@author: Xiang Huo
'''

import sys
import ls
import redis
import imte
from datetime import datetime, timedelta
import psycopg2 as pg
import glob
import gzip
import logging as Log

con = None
r = redis.StrictRedis(host='localhost', port=6379,db=0)
Log.basicConfig(filename='./http2db.log',format='%(asctime)s %(message)s', level=Log.INFO)
tcolumns = '(ts, orig_h, resp_h, host, uri, referrer, method, user_agent, status_code)'

try:
    con = pg.connect(database='tds',user='tds',host='localhost',password='9bBJPLr9')
    con.autocommit = True
    cur = con.cursor()
    
except pg.DatabaseError, e:
    Log.error(e.pgerror)
    sys.exit(1)

def postProcess():
    global cur
    date = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
    tdate = (datetime.new() + timedelta(days=-1)).strftime('%Y-%m-%d')
    ndate = datetime.now().strftime('%Y-%m-%d')
    prev_date = (datetime.now() + timedelta(days=-2)).strftime('%Y%m%d')
    tname_date = 'log_' + date
    tname_prev_date = 'log_' + prev_date
    try:
        cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='tds' and table_schema='public' and table_name='%s')" % tname_date)
        tname_date_exists = str(cur.fetchone()[0])
        cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='tds' and table_schema='public' and table_name='%s')" % tname_prev_date)
        tname_prev_date_exists = str(cur.fetchone()[0])
    except pg.DatebaseError, e:
        Log.error(e.pgerror)
        sys.exit(1)
    if tname_date_exists == 'True' and tname_prev_date_exists == 'True':
        try:
            cur.execute("INSERT INTO %s %s SELECT %s FROM %s WHERE to_char(ts, 'YYYYMMDD') = '%s'" %(tname_prev_date, tcolumns, tcolumns[1:-1], tname_date, prev_date))
            cur.execute("DELETE FROM %s WHERE to_char(ts, 'YYYYMMDD') = '%s'" %(tname_date, prev_date))
            cur.execute("ALTER TABLE %s ADD CONSTRAINT date CHECK (ts >= '%s' AND ts < '%s')" %(tname_date, tdate, ndate))
            cur.execute('CREATE INDEX ON %s(resp_h)' %tname_date)
            cur.execute('CREATE INDEX ON %s(host)' %tname_date)
            cur.execute('CREATE INDEX ON %s(md5(uri))' %tname_date)
            Log.info('indexes were successfully created')
        except pg.DatabaseError, e:
            Log.error(e.pgerror)
            sys.exit(1)
    else:
        Log.info('postprocessing of %s table was skipped', tname_date);

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
                args = ','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s)',r) for r in records)
                try:
                    cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                except pg.DatabaseError, e:
                    Log.error('%s : %s' %(lfile, e.pgerror))
            Log.info('%d records were logged to db from %s' %(ctr, lfile))
            logfile.close()
            return
        fields = line.split('\t')
        if filed[0] == '#fields':
            fieldnames = fields[1:]
            continue
        if fields[0].find('#') == 0:
            continue
        keyval = dict(zip(fieldnames,fields))
        try:
            (records.append((keyval['ts'], keyval['id.orig_h'], keyval['id.resp_h'], keyval['host'][:256], keyval['url'][:65536], keyval['referrer'][:65536], keyval['method'][:16], keyval['user_agent'][:2048], keyval['status_code'][:16])))
        except Exception, e:
            Log.error('%s : %s' %(lfile,e))
            pass
        if ctr % 10000 == 0:
            args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)",r) for r in records)
            try:
                cur.execute('INSERT INTO %s %s VALUES %s' %(tname, tcolumns, args))
                del records[:]
                args = ''
            except pg.DatabaseError, e:
                Log.error('%s : %s' %(lfile, e.pgerror))
                continue


def main():
    create_new_table = '''CREATE TABLE %s
    (
     id serial primary key NOT NULL
    ) INHERITS (gmuhttplog)'''
    test = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')
    print test
    data_to_process = 'i12'
    logdir = '/raid/brolog/%s' % data_to_process
    if os.path.exists(logdir):
        tname = 'log_' + data_to_process
        try:
            cur.execute(creats_new_table % tname)
        except pg.DatabaseError, e:
            Log.error('Creating new table %s failed : %s' %(tname, e.pgerror))
            sys.exit(1)
        logfiles = glob.glob(logdir + 'http-request*.gz')
        if logfiles:
            for logfile in logfiles:
                log2db(logfile, tname)
            postProcess()
    else:
        Log.info('No log directory was found %s' % data_to_process)
        sys.exit(0)
if __name__ =='__main__':main()

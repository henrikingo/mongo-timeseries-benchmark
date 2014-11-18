#!/usr/bin/python

# Copyright 2013, MongoDB Inc
# Authors: Henrik Ingo, Chris Biow
#
# Simulate time-series data feed and queries with MongoDB
#
# 1. Do db.tstest.drop() and recreate any indexes you think you need.
# 2. Just run ./timeSeriesTest.py - currently it does the data loading and queries all in one. (TODO is to separate them with command line arguments.)
# 3. Configurable variables are below
# 4. To use different schema and/or data loading method or multi-threading, provide your own setup_f() and load_f() functions
# 5. To test against a different database than MongoDB, provide a complete dbdriver module (load and queries)

starttime     = 1388432485     # timestamp in the first batch to load (1388432485)
ts_interval   = 60             # artificial time between each batch to load (60)
iterations    = 6000           # how many batches to load (6000) Note: for queries 2 and 3 to work this has to be > 400
batch_size    = 100*1000       # rows in a batch (100*1000) (cols is a fixed 30)
csvHeader     = "device_id,ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28\n"

# You can, and should, create different loader functions to explore better performance with MongoDB or to port to other DBs
dbhost = 'localhost'
dbport = '27017'
dbname = 'test'
import timeSeriesTestMongoDB as dbdriver
setup_f       = dbdriver.writeCsvFile   # write the data to a csv file on disk
load_f        = dbdriver.csvMongoimport # load the csv file with mongoimport
query1        = dbdriver.query1         # select 1 random column  where device_id in 4 random id's
query2        = dbdriver.query2         # select 1 random column  where device_id in 4 random ids AND timestamp between t and t+400*ts_interval
query3        = dbdriver.query3         # select 4 random columns where device_id in 4 random ids AND timestamp between t and t+400*ts_interval

###############################################################################

import random
import sys
import time



def exportGlobals() :
    """Push global config vars to the sub module that needs them. Apparently there's no good way in Python to do this."""
    dbdriver.starttime     = starttime
    dbdriver.ts_interval   = ts_interval
    dbdriver.iterations    = iterations
    dbdriver.batch_size    = batch_size
    dbdriver.csvHeader     = csvHeader
    dbdriver.dbhost        = dbhost
    dbdriver.dbport        = dbport
    dbdriver.dbname        = dbname

exportGlobals()


def getCsvBatch(timestamp) :
    """Get 100k rows with an id field, timestamp field and 28 columns with random integers."""
    random.seed(42) # Causes the generated numbers to be exactly the same for each run. Not really significant but it's a habit I have...
    ret = csvHeader
    
    for i in range(0, batch_size) :
        line = "%s,%s" % (i, int(timestamp))
        for j in range(0, 27) :
            r = random.randint(0,1000)
            line += ",%s" % r
        ret += line + "\n"
    
    return ret


def timeBatchLoading(csv, f, setup=None) :
    """Call function f, which should load csv into mongo, and time it.
    
       If setup() is given, first call r = setup(csv) and then f(r) with return value of setup. Only f() is timed."""
    if(setup) :
        csv = setup(csv)
    start = time.time()
    f(csv)
    return time.time() - start

def timeQuery(q) :
    """Call q() and time it."""
    start = time.time()
    q()
    return time.time() - start





# main
print "Start timeSeriesTest.py dataload with setup function '%s' and load function '%s'." % (setup_f, load_f)
print "Note: It is your responsibility to drop your test collection/table and recreate it/indexes before running this test."
print "      If you don't, you will get errors."

t = starttime
timings = []
for i in range(1, iterations+1) :
  t = t + ts_interval
  csv = getCsvBatch(t)
  print "\nStarting load %s..." % i
  timer = timeBatchLoading(csv, load_f, setup_f)
  print "Load %s took: %s sec." % (i, timer)
  timings.append(timer)
  sys.stdout.flush()
  
print "\nAll %s load times were:" % iterations 
for s in timings :
  print "%s" % s
sys.stdout.flush()

# Run each query 100 times
timings = []
for i in range(0, 100) :
    #print "\nExecuting query1"
    timer = timeQuery(query1)
    timings.append(timer)
    #print "query1 took: %s" % timer
print "\nAll 100 timings for query1 were:"
for s in timings :
  print "%s" % s
sys.stdout.flush()

timings = []
for i in range(0, 100) :
    #print "\nExecuting query2"
    timer = timeQuery(query2)
    timings.append(timer)
    #print "query2 took: %s" % timer
print "\nAll 100 timings for query2 were:"
for s in timings :
  print "%s" % s
sys.stdout.flush()

timings = []
for i in range(0, 100) :
    #print "\nExecuting query3"
    timer = timeQuery(query3)
    timings.append(timer)
    #print "query3 took: %s" % timer
print "\nAll 100 timings for query3 were:"
for s in timings :
  print "%s" % s
sys.stdout.flush()

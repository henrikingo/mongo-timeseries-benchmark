#!/usr/bin/python

# Copyright 2013-2014, MongoDB Inc
# Authors: Henrik Ingo
#
# Simulate time-series data feed and queries with MongoDB
#
# 1. Do db.tstest.drop() and recreate any indexes you think you need.
# 2. Just run ./timeSeriesTest.py - currently it does the data loading and queries all in one. (TODO is to separate them with command line arguments.)
# 3. Configurable variables are below
# 4. To use different schema and/or data loading method or multi-threading, provide your own prepare_f() and load_f() functions
# 5. To test against a different database than MongoDB, provide a complete dbdriver module (load and queries)

config = {}

# Test parameters
config["starttime"]     = 1388432485     # timestamp in the first batch to load (1388432485)
config["ts_interval"]   = 60             # artificial time between each batch to load (60)
config["iterations"]    = 600           # how many batches to load (6000) Note: for queries 2 and 3 to work this has to be > 400
config["batch_size"]    = 100       # rows in a batch (100*1000) (cols is a fixed 30)
config["csvHeader"]     = "device_id,ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28\n"

# DB vendor specific parameters
# If you want to run only some part of the test, set any of the prepare_f, load_f, query1... to a false value.
# You can, and should, create different loader functions to explore better performance with MongoDB or to port to other DBs
config["dbhost"] = 'localhost'
config["dbport"] = '27017'
config["dbname"] = 'test'
import timeSeriesTestMongoDB as dbdriver
config["init_f"]        = dbdriver.init           # drop collection, create indexes, etc... (only run if load_f is also given)
config["prepare_f"]     = dbdriver.writeCsvFile   # write the data to a csv file on disk
config["load_f"]        = dbdriver.csvMongoimport # load the csv file with mongoimport
config["tests"]         = [None]*3
config["tests"][0]      = dbdriver.query1         # select 1 random column  where device_id in 4 random id's
config["tests"][1]      = dbdriver.query2         # select 1 random column  where device_id in 4 random ids AND timestamp between t and t+400*ts_interval
config["tests"][2]      = dbdriver.query3         # select 4 random columns where device_id in 4 random ids AND timestamp between t and t+400*ts_interval

###############################################################################

import random
import sys
import time
import pprint


#Push global config vars to the sub module that needs them.
dbdriver.config     = config


def getCsvBatch(timestamp) :
    """Get 100k rows with an id field, timestamp field and 28 columns with random integers."""
    random.seed(42) # Causes the generated numbers to be exactly the same for each run. Not really significant but it's a habit I have...
    ret = config["csvHeader"]
    
    for i in range(0, config["batch_size"]) :
        line = "%s,%s" % (i, int(timestamp))
        for j in range(0, 27) :
            r = random.randint(0,1000)
            line += ",%s" % r
        ret += line + "\n"
    
    return ret


def timeBatchLoading(data, f, prepare=None) :
    """Call function f, which should load csv into mongo, and time it.
    
       If prepare() is given, first call r = prepare(csv) and then f(r) with return value of prepare. Only f() is timed."""
    if(prepare) :
        data = prepare(data)
    start = time.time()
    f(data)
    return time.time() - start

def timeQuery(q) :
    """Call q() and time it."""
    start = time.time()
    q()
    return time.time() - start





# main
print "Start timeSeriesTest.py dataload with following config: "
pprint.pprint(config)

# Data load
if config["load_f"] :
  config["init_f"]()
  t = config["starttime"]
  timings = []
  for i in range(1, config["iterations"]+1) :
    t = t + config["ts_interval"]
    data = getCsvBatch(t)  # TODO: Even this data generation function could become dynamic, so you could generate different data sets
    print "\nStarting load %s..." % i
    timer = timeBatchLoading(data, config["load_f"], config["prepare_f"])
    print "Load %s took: %s sec." % (i, timer)
    timings.append(timer)
    sys.stdout.flush()
  
  print "\nAll %s load times were:" % config["iterations"] 
  for s in timings :
    print "%s" % s
  sys.stdout.flush()

# Run each test 100 times
# TODO: Add configurability for iterations and parallellism
for test in config["tests"] :
  timings = []
  print "\nExecuting %s 100 times." % test
  for i in range(0, 100) :
      timer = timeQuery(test)
      timings.append(timer)
      #print "took: %s" % timer
  print "\nAll 100 timings for %s were:" % test
  for s in timings :
    print "%s" % s
  sys.stdout.flush()


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
config["starttime"]       = 1388432485     # timestamp in the first batch to load (1388432485)
config["ts_interval"]     = 60             # artificial time between each batch to load (60)
config["batches"]         = 6000           # how many batches to load (6000) Note: for queries 2 and 3 to work this has to be > 400
config["batch_size"]      = 100*1000       # rows in a batch (100*1000) (cols is a fixed 30)
config["csvHeader"]       = "device_id,ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28\n"
config["load_threads"]    = 8              # Parallel load threads (using multiprocessing module)
config["test_threads"]    = 1              # Parallel query threads (using multiprocessing module)
config["test_iterations"] = 100            # how many iterations is each test query executed

# DB vendor specific parameters
# If you want to run only some part of the test, set any of the prepare_f, load_f, query1... to a false value.
# You can, and should, create different loader functions to explore better performance with MongoDB or to port to other DBs
config["dbhost"] = 'localhost'
config["dbport"] = '27017'
config["dbname"] = 'test'
import timeSeriesTestMongoDB as dbdriver
config["init_f"]        = dbdriver.init           # drop collection, create indexes, etc... (only run if load_f is also given)
config["prepare_f"]     = dbdriver.csvToArray     # write the data to a csv file on disk
config["load_f"]        = dbdriver.arrayInsert    # load the csv file with mongoimport
config["tests"]         = [None]*3
config["tests"][0]      = dbdriver.query1         # select 1 random column  where device_id in 4 random id's
config["tests"][1]      = dbdriver.query2         # select 1 random column  where device_id in 4 random ids AND timestamp between t and t+400*ts_interval
config["tests"][2]      = dbdriver.query3         # select 4 random columns where device_id in 4 random ids AND timestamp between t and t+400*ts_interval

###############################################################################

import random
import sys
import time
import pprint
from   multiprocessing import Pool
import math

#Push global config vars to the sub module that needs them.
dbdriver.config     = config


def getCsvBatch(paramArray) :
    """Get 100k rows with an id field, timestamp field and 28 columns with random integers."""
    random.seed(42) # Causes the generated numbers to be exactly the same for each run. Not really significant but it's a habit I have...

    ret = config["csvHeader"]
    
    # For multiple load threads, calculate the partition of device_id's this thread will generate
    batch_size_per_thread = config["batch_size"] / config["load_threads"]
    device_id_start       = int( math.floor( paramArray["i"] * batch_size_per_thread ) ) 
    device_id_end         = int( math.floor( ( paramArray["i"] + 1 ) * batch_size_per_thread ) )
    # For the last thread, make sure the upper end is correctly rounded up/down wrt config parameter
    if paramArray["i"] == config["load_threads"] - 1 :
      device_id_end = config["batch_size"]

    for i in range( device_id_start, device_id_end ) :
        line = "%s,%s" % (i, int(paramArray["t"]))
        for j in range(0, 27) :
            r = random.randint(0,1000)
            line += ",%s" % r
        ret += line + "\n"
    
    return ret


def timeLoading(t, f, prepare=None) :
    """Call function f, which should load csv into mongo, and time it.
    
       If prepare() is given, first call r = prepare(csv) and then f(r) with return value of prepare. Only f() is timed."""
    
    paramArray = []
    for i in range( 0, config["load_threads"] ) :
      paramArray.append( { "t" : t, "i" : i } )
    
    csv_results = pool_load.map( getCsvBatch, paramArray, 1 )
    if(prepare) :
        prepare_results = pool_load.map( prepare, csv_results )
    start = time.time()
    pool_load.map( f, prepare_results )
    return time.time() - start

def timeQuery(q) :
    """Call q() and time it."""
    paramArray = [None]*config["test_threads"]
    start = time.time()
    pool_test.map( q, paramArray, 1)
    return time.time() - start



pool_load = None
pool_test = None

if __name__ == '__main__':
  config['client_name']    = dbdriver.getClientName()
  config['client_version'] = dbdriver.getClientVersion()
  config['server_name']    = dbdriver.getServerName()
  config['server_version'] = dbdriver.getServerVersion()
  print "Start timeSeriesTest.py dataload with following config: "
  pprint.pprint(config)

  # Data load
  if config["load_f"] :
    pool_load = Pool( processes=config["load_threads"] )
    config["init_f"]()
    t = config["starttime"]
    timings = []
    for i in range(1, config["batches"]+1) :
      t = t + config["ts_interval"]
      print "\nStarting load %s..." % i
      timer = timeLoading(t, config["load_f"], config["prepare_f"])
      print "Load %s took: %s sec." % (i, timer)
      timings.append(timer)
      sys.stdout.flush()
    
    print "\nResults for all %s load times were:" % config["batches"] 
    import numpy
    print "Average (s)    :\t%s" % numpy.average(timings)
    print "Variance (s^2) :\t%s" % numpy.var(timings)
    print "Variance/Mean  :\t%s" % ( numpy.var(timings) / numpy.average(timings) )
    print "Min            :\t%s" % numpy.min(timings)
    print "Median (50%%)   :\t%s" % numpy.median(timings)
    print "90%% percentile :\t%s" % numpy.percentile(timings, 0.9)
    print "95%% percentile :\t%s" % numpy.percentile(timings, 0.95) 
    print "98%% percentile :\t%s" % numpy.percentile(timings, 0.98) 
    print "99%% percentile :\t%s" % numpy.percentile(timings, 0.99) 
    print "Max            :\t%s" % numpy.max(timings)
    print
    print "Time\trows/sec"    
    for s in timings :
      print "%s\t%s" % (s, config["batch_size"]/s) 
    sys.stdout.flush()

  # Run each test config["test_iterations"] times
  pool_test = Pool( processes=config["test_threads"] )
  for test in config["tests"] :
    timings = []
    print "\nExecuting %s %s times." % (test, config["test_iterations"])
    for i in range(0, config["test_iterations"]) :
        timer = timeQuery(test)
        timings.append(timer)

    print "\nResults for %s iterations of %s were:" % (config["test_iterations"], test)
    print "Average (s)    :\t%s" % numpy.average(timings)
    print "Variance (s^2) :\t%s" % numpy.var(timings)
    print "Variance/Mean  :\t%s" % ( numpy.var(timings) / numpy.average(timings) )
    print "Min            :\t%s" % numpy.min(timings)
    print "Median (50%%)   :\t%s" % numpy.median(timings)
    print "90%% percentile :\t%s" % numpy.percentile(timings, 0.9)
    print "95%% percentile :\t%s" % numpy.percentile(timings, 0.95) 
    print "98%% percentile :\t%s" % numpy.percentile(timings, 0.98) 
    print "99%% percentile :\t%s" % numpy.percentile(timings, 0.99) 
    print "Max            :\t%s" % numpy.max(timings)
    print
    for s in timings :
      print "%s" % s
    sys.stdout.flush()


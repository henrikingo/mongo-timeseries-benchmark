#!/usr/bin/python

# Copyright 2013, MongoDB Inc
# Authors: Henrik Ingo, Chris Biow
#
# This module contains functions to be used as setup_f() and load_f() in timeSeriesTest.py

# A bit unusual, but I set these in the main module with exportGlobals()
starttime     = None
ts_interval   = None
iterations    = None
batch_size    = None
csvHeader     = None
dbhost        = None
dbport        = None
dbname        = None


import tempfile

import pymongo

def getMongoDB() :
    client = pymongo.MongoClient( "mongodb://%s:%s/" % (dbhost, dbport) )
    return client[dbname]


# Load data with mongoimport

def writeCsvFile(csv) :
    """Write csv to a temporary file, return file object."""
    f = tempfile.NamedTemporaryFile()
    f.write(csv)
    f.flush()
    return f

import os

def csvMongoimport(f) :
    """Import CSV file with mongoimport"""
    os.system( "mongoimport --db test --collection tstest --type csv --headerline --file %s" % f.name )
    f.close()
    


# Load data with simple inserts

import pprint
def csvToArray(csv) :
    csvArray = []
    csvHeaders = []
    lineCount = 0
    for line in csv.split("\n") :
        if lineCount == 0 :
            csvHeaders = line.split(",")
            lineCount = lineCount + 1
            continue
        if lineCount > batch_size :
            break # The last newline causes there to be an empty record batch_size+1, which breaks things if allowed to proceed
        doc = {}        
        colCount = 0
        for v in line.split(",") :
            doc[ csvHeaders[colCount] ] = int(v)
            colCount = colCount + 1
        csvArray.append(doc)
        lineCount = lineCount + 1
    return csvArray
        
def simpleInsert(csvArray) :
    db = getMongoDB()
    for doc in csvArray :
        db.tstest.insert( doc )        




import random    

def query1() :
    """select 1 random column  where device_id in 4 random id's"""
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, batch_size-1) )
    cols = csvHeader.split(",")
    col = cols[ random.randint(2,28) ]
    
    cursor = db.tstest.find( { "device_id" : { "$in" : ids } }, { col : 1 } )
    c = cursor.count()
    #if c != iterations*3 :
    #    print "\nERROR: result set for query1 is %s, expected %s." % (c, 3*iterations)
    #    pprint.pprint(ids)
    
def query2() :
    """select 1 random column  where device_id in 4 random ids AND timestamp between t and t+400*ts_interval"""
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, batch_size-1) )
    cols = csvHeader.split(",")
    col = cols[ random.randint(2,28) ]
    
    t = random.randint(starttime, starttime+iterations*ts_interval-400*ts_interval)
    
    cursor = db.tstest.find( { "device_id" : { "$in" : ids }, 
                               "ts" : { "$gte" : t, "$lte" : t+400*ts_interval } }, 
                             { col : 1 } )
    c = cursor.count()
    #if c != 3*400 :
    #    print "\nERROR: result set for query2 is %s, expected %s." % (c, 3*400)
    #    pprint.pprint(ids)
    
def query3() :
    """select 4 random columns where device_id in 4 random ids AND timestamp between t and t+400*ts_interval"""
    # Note: the point of this query is mostly to make life harder for columnar databases, while for a doc/row oriented db query2 and query3 are pretty much the same 
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, batch_size-1) )
    cols = csvHeader.split(",")
    col1 = cols[ random.randint(2,28) ]
    col2 = cols[ random.randint(2,28) ]
    col3 = cols[ random.randint(2,28) ]
    col4 = cols[ random.randint(2,28) ]

    t = random.randint(starttime, starttime+iterations*ts_interval-400*ts_interval)
        
    cursor = db.tstest.find( { "device_id" : { "$in" : ids }, 
                               "$and" : [ { "ts" : { "$gt" : t } }, { "ts" : { "$lt" : t+400*ts_interval } } ] }, 
                             { col1 : 1, col2 : 1, col3 : 1, col4 : 1 } )
    c = cursor.count()
    #if c != 3*400 :
    #    print "\nERROR: result set for query2 is %s, expected %s." % (c, 3*400)
    #    pprint.pprint(ids)


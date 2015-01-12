#!/usr/bin/python

# Copyright 2013-2014, MongoDB Inc
# Authors: Henrik Ingo
#
# This module contains MongoDB specific functions. 
# Set which functions to use in the top of timeSeriesTest.py (e.g. config["prepare_f"], config["load_f"])

# Will contain global config vars. Set in the main module.
config        = None


import tempfile
import pymongo

# provide mongod and pymongo version so that it is recorded as part of the inital output
def getClientName() :
  return "pymongo"

def getClientVersion() :
  return pymongo.version

def getServerName() :
  return "mongodb"

def getServerVersion() :
  client = pymongo.MongoClient( "mongodb://%s:%s/" % (config["dbhost"], config["dbport"]) )
  return (client.server_info())['version']
  


def getMongoDB() :
    client = pymongo.MongoClient( "mongodb://%s:%s/" % (config["dbhost"], config["dbport"]) )
    return client[config["dbname"]]




# Test initialization
def init() :
  db = getMongoDB()
  db.tstest.drop()
  db.tstest.ensure_index( [ ("device_id", 1), ("ts", 1)] )


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
    os.system( "mongoimport --db %s --collection tstest --type csv --headerline --file %s" % (config["dbname"], f.name) )
    f.close() # temporary file automatically removed
    


# Load data with simple inserts

import pprint
def csvToArray(csv) :
    docArray = []
    csvHeaders = []
    lineCount = 0
    lines = csv.split("\n")
    lines.pop() # Since the last line ends with a newline, there's an empty list item at the end. Get rid of it.
    for line in lines :
        if lineCount == 0 :
            csvHeaders = line.split(",")
            lineCount = lineCount + 1
            continue
        doc = {}        
        colCount = 0
        for v in line.split(",") :
            doc[ csvHeaders[colCount] ] = int(v)
            colCount = colCount + 1
        docArray.append(doc)
        lineCount = lineCount + 1
    return docArray
        
def simpleInsert(docArray) :
    db = getMongoDB()
    for doc in docArray :
        db.tstest.insert( doc )        

def arrayInsert(docArray) :
    db = getMongoDB()
    db.tstest.insert( docArray )        



import random    

def query1(arg) :
    """select 1 random column  where device_id in 4 random id's"""
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, config["batch_size"]-1) )
    cols = config["csvHeader"].split(",")
    col = cols[ random.randint(2,28) ]
    
    cursor = db.tstest.find( { "device_id" : { "$in" : ids } }, { col : 1 } )
    c = cursor.count()
    #if c != iterations*3 :
    #    print "\nERROR: result set for query1 is %s, expected %s." % (c, 3*iterations)
    #    pprint.pprint(ids)
    
def query2(arg) :
    """select 1 random column  where device_id in 4 random ids AND timestamp between t and t+400*ts_interval"""
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, config["batch_size"]-1) )
    cols = config["csvHeader"].split(",")
    col = cols[ random.randint(2,28) ]
    
    t = random.randint(config["starttime"], 
                       config["starttime"]+config["iterations"]*config["ts_interval"]-400*config["ts_interval"])
    
    cursor = db.tstest.find( { "device_id" : { "$in" : ids }, 
                               "ts" : { "$gte" : t, "$lte" : t+400*config["ts_interval"] } }, 
                             { col : 1 } )
    c = cursor.count()
    #if c != 3*400 :
    #    print "\nERROR: result set for query2 is %s, expected %s." % (c, 3*400)
    #    pprint.pprint(ids)
    
def query3(arg) :
    """select 4 random columns where device_id in 4 random ids AND timestamp between t and t+400*ts_interval"""
    # Note: the point of this query is mostly to make life harder for columnar databases, while for a doc/row oriented db query2 and query3 are pretty much the same 
    db = getMongoDB()
    ids = []
    for i in range(0, 3) :
        ids.append( random.randint(0, config["batch_size"]-1) )
    cols = config["csvHeader"].split(",")
    col1 = cols[ random.randint(2,28) ]
    col2 = cols[ random.randint(2,28) ]
    col3 = cols[ random.randint(2,28) ]
    col4 = cols[ random.randint(2,28) ]

    t = random.randint(config["starttime"], 
                       config["starttime"]+config["iterations"]*config["ts_interval"]-400*config["ts_interval"])
        
    cursor = db.tstest.find( { "device_id" : { "$in" : ids }, 
                               "$and" : [ { "ts" : { "$gt" : t } }, { "ts" : { "$lt" : t+400*config["ts_interval"] } } ] }, 
                             { col1 : 1, col2 : 1, col3 : 1, col4 : 1 } )
    c = cursor.count()
    #if c != 3*400 :
    #    print "\nERROR: result set for query2 is %s, expected %s." % (c, 3*400)
    #    pprint.pprint(ids)


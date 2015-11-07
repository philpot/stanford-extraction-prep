#!/usr/bin/env python

### TEST WITH THIS NUMBER (818) 200-6476
### TEST WITH THIS VERY COMMON NUMBER (415) 683-3245

try:
    from pyspark import SparkContext, SparkFiles
except:
    print "### NO PYSPARK"
import sys
import os
import platform
import socket
import argparse
import json
from itertools import izip, izip_longest, count
import time
from datetime import timedelta
from random import randint


### from trollchar.py

def asList(x):
    if isinstance(x, list):
        return x
    else:
        return [x]

### from util.py

def iterChunks(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

### end from util.py
# Sniff for execution environment

location = "hdfs"
try:
    if "avatar" in platform.node():
        location = "local"
except:
    pass
try:
    if "avatar" in socket.gethostname():
        location = "local"
except:
    pass
print "### location %s" % location


configDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "data/config")
def configPath(n):
    return os.path.join(configDir, n)

binDir = os.getcwd() if location=="hdfs" else os.path.join(os.path.dirname(__file__), "bin")
def binPath(n):
    return os.path.join(binDir, n)

# Adapted from Dipsy's list from dig-aligment ht version 1.0

sourceById = {
"1":	"backpage",
"2":	"craigslist",
"3":	"classivox",
"4":	"myproviderguide",
"5":	"naughtyreviews",
"6":	"redbook",
"7":	"cityvibe",
"8":	"massagetroll",
"9":	"redbookforum",
"10":   "cityxguide",
"11":   "cityxguideforum",
"12":   "rubads",
"13":   "anunico",
"14":   "sipsap",
"15":   "escortsincollege",
"16":   "escortphonelist",
"17":   "eroticmugshots",
"18":   "escortadsxxx",
"19":   "escortsinca",
"20":   "escortsintheus",
"21":   "liveescortreviews",
"22":   "myproviderguideforum",
"23":   "usasexguide",
"24":   "theeroticreview",
"25":   "adultsearch",
"26":   "happymassage",
"27":   "utopiaguide",
"28":   "missing kids",
"29":   "alibaba",
"30":   "justlanded",
"31":   "gmdu",
"32":   "tradekey",
"33":   "manpowervacancy",
"34":   "gulfjobsbank",
"35":   "ec21"
}

sourceByName = {
"backpage":	"1",
"craigslist":	"2",
"classivox":	"3",
"myproviderguide":	"4",
"naughtyreviews":	"5",
"redbook":	"6",
"cityvibe":	"7",
"massagetroll":	"8",
"redbookforum":	"9",
"cityxguide":   "10",
"cityxguideforum":   "11",
"rubads":   "12",
"anunico":   "13",
"sipsap":   "14",
"escortsincollege":   "15",
"escortphonelist":   "16",
"eroticmugshots":   "17",
"escortadsxxx":   "18",
"escortsinca":   "19",
"escortsintheus":   "20",
"liveescortreviews":   "21",
"myproviderguideforum":   "22",
"usasexguide":   "23",
"theeroticreview":   "24",
"adultsearch":   "25",
"happymassage":   "26",
"utopiaguide":   "27",
"missing kids":   "28",
"alibaba":   "29",
"justlanded":   "30",
"gmdu":   "31",
"tradekey":   "32",
"manpowervacancy":   "33",
"gulfjobsbank":   "34",
"ec21":   "35"
}

"""
adultsearch
backpage
cityvibe
cityxguide
classivox
craigslist
escortadsxxx
escortphonelist
escortsinca
escortsincollege
escortsintheus
massagetroll
myproviderguide
redbook
rubads
sipsap
usasexguide
utopiaguide
"""

"""
adultsearch
backpage
cityvibe
cityxguide
classivox
craigslist
escortadsxxx
escortphonelist
escortsinca
escortsincollege
escortsintheus
massagetroll
myproviderguide
redbook
rubads
sipsap
usasexguide
utopiaguide
"""

def getSourceById(id):
    return sourceById.get(id, "unknownsourceid_{}".format(id))

def getSourceByName(name):
    return sourceByName.get(name, "unknownsourcename_{}".format(name))

def prep(sc, cdr, stanford, successOutput, failOutput,
         # how many prefix fields to drop from stanford input
         shift = 0,
         uriClass='Offer',
         # minimum initial number of partitions
         numPartitions=None, 
         limit=None, 
         debug=0, 
         location='hdfs', 
         outputFormat="text",
         sampleSeed=1234,
         # Cheat means artificially limit to only those cases covering known number (415) 683-3245
         cheat=False):

    show = True if debug>=1 else False
    def showPartitioning(rdd):
        """Seems to be significantly more expensive on cluster than locally"""
        if show:
            partitionCount = rdd.getNumPartitions()
            try:
                valueCount = rdd.countApprox(1000, confidence=0.50)
            except:
                valueCount = -1
            print ("At %s, there are %d partitions with on average %s values" % 
                   (rdd.name(), partitionCount, int(valueCount/float(partitionCount))))

    debugOutput = successOutput + '_debug'
    def debugDump(rdd,keys=True,listElements=False):
        showPartitioning(rdd)
        keys=False
        if debug >= 2:
            startTime = time.time()
            outdir = os.path.join(debugOutput, rdd.name() or "anonymous-%d" % randint(10000,99999))
            keyCount = None
            try:
                keyCount = rdd.keys().count() if keys else None
            except:
                pass
            rowCount = None
            try:
                rowCount = rdd.count()
            except:
                pass
            elementCount = None
            try:
                elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
            except:
                pass
            rdd.saveAsTextFile(outdir)
            endTime = time.time()
            elapsedTime = endTime - startTime
            print "wrote [%s] to outdir %r: [%s, %s, %s]" % (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount)

    def showSizeAndExit(rdd):
        try:
            k = rdd.count()
        except:
            k = None
        print "Just finished %s with size %s" % (rdd.name(), k)
        exit(0)

    rdd_cdr = sc.textFile(cdr)
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_cdr.count()
        rdd_cdr = rdd_cdr.sample(False, ratio, seed=sampleSeed)
    rdd_cdr.setName('cdr')
    debugDump(rdd_cdr)
    
    def splitCdrLine(line):
        (url, jdata) = line.split('\t')
        d = json.loads(jdata)
        # sid = d["_source"]["sid"]
        sourceId = d["_source"]["sources_id"]
        incomingId = d["_source"]["incoming_id"]
        id = d["_source"]["id"]
        url = d["_source"]["url"]
        return ( (sourceId, incomingId), (id, url) )
    rdd_cdr_split = rdd_cdr.map(lambda line: splitCdrLine(line))
    rdd_cdr_split.setName('rdd_cdr_split')
    debugDump(rdd_cdr_split)

    rdd_cdr_sort = rdd_cdr_split.sortByKey()
    rdd_cdr_sort.setName('rdd_cdr_sort')
    debugDump(rdd_cdr_sort)

    rdd_stanford = sc.textFile(stanford)
    rdd_stanford.setName('rdd_stanford')
    if limit:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd_stanford.count()
        rdd_stanford = rdd_stanford.sample(False, ratio, seed=sampleSeed)

    # temp
    if cheat:
        rdd_stanford = rdd_stanford.filter(lambda line: "(415) 683" in line)

    rdd_stanford.setName('stanford')
    debugDump(rdd_stanford)


    def splitStanfordLine(line):
        fields = line.split('\t')[shift:]
        sourceNameCrawlId, valuesExpr = fields
        (sourceName, crawlId) = sourceNameCrawlId.split(":")
        sourceId = getSourceByName(sourceName)
        try:
            sourceId = int(sourceId)
        except:
            pass
        try:
            crawlId = int(crawlId)
        except:
            pass
        values = valuesExpr.split(',')
        return ( (sourceId, crawlId), tuple(values) )

    rdd_stanford_split = rdd_stanford.map(lambda line: splitStanfordLine(line))
    rdd_stanford_split.setName('rdd_stanford_split')
    debugDump(rdd_stanford_split)

    rdd_stanford_sort = rdd_stanford_split.sortByKey()
    rdd_stanford_sort.setName('rdd_stanford_sort')
    debugDump(rdd_stanford_sort)

    # all stanford gets a CDR tag
    # rdd_net = rdd_stanford_sort.leftOuterJoin(rdd_cdr_sort)
    # elements look like
    # (sourceIdInt, crawlIdInt) => ( <stanfordValuesTuple>, <cdrValuesTuple> )
    # ((4, 129640), ((u'(415) 683-3245',), (346752, u'http://www.myproviderguide.com/escorts/san-francisco/free-posts/w4m/5686036_outcalls-only-silm-girl-korean.html')))
    # where <stanfordValuesTuple> can be None
    # where <cdrValuesTuple> can be None
    # this is what we might want
    # left outer join:
    # keep everything from stanford, even if no match from CDR
    rdd_net = rdd_stanford_sort.leftOuterJoin(rdd_cdr_sort)
    rdd_net.setName('rdd_net')
    debugDump(rdd_net)

    # successful are those where cdr ID is not None)
    rdd_success = rdd_net.filter(lambda r: r[1] and r[1][1] and r[1][1][0])
    print "Success {} tuples".format(rdd_success.count())
    rdd_success.setName('rdd_success')
    debugDump(rdd_success)

    rdd_fail = rdd_net.filter(lambda r: not (r[1] and r[1][1] and r[1][1][0]))
    print "Fail {} tuples".format(rdd_fail.count())

    def emitJson(r):
        (k, payload) = r
        (sourceId, crawlId) = k
        (stanfordValues, cdrValues) = payload
        adId = None
        url = None
        try:
            (adId, url) = cdrValues
        except:
            pass
        d = {}
        if sourceId:
            d["sourceId"] = sourceId
        sourceName = None
        try:
            sourceName = getSourceById(str(sourceId))
        except:
            pass
        if sourceName:
            d["sourceName"] = sourceName
        if crawlId:
            d["crawlId"] = crawlId
        if adId:
            d["adId"] = adId
        if url:
            d["url"] = url
        for (value, idx) in izip(stanfordValues, count(1)):
            if value:
                d["stanfordExtraction{}".format(idx)] = value
        return d

    def jsonRow(r):
        j = emitJson(r)
        return (j.get("url", "noUrlAvailable"), j)

    # rdd_success_json = rdd_success.map(lambda r: (json.dumps(emitJson(r)))
    rdd_success_json = rdd_success.map(lambda r: jsonRow(r))

    if rdd_success_json.isEmpty():
        print "### NO SUCCESS DATA TO WRITE"
    else:
        if outputFormat == "sequence":
            rdd_success_json.saveAsSequenceFile(successOutput)
        elif outputFormat == "text":
            rdd_success_json.saveAsTextFile(successOutput)
        elif outputFormat == "tsv":
            rdd_tsv = rdd_success_json.map(lambda (k,p): k + "\t" + p[0] + "\t" + p[1])
            rdd_tsv.saveAsTextFile(successOutput)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)

    # rdd_fail_json = rdd_fail.map(lambda r: json.dumps(emitJson(r)))
    rdd_fail_json = rdd_fail.map(lambda r: jsonRow(r))

    if rdd_fail_json.isEmpty():
        print "### NO FAIL DATA TO WRITE"
    else:
        if outputFormat == "sequence":
            rdd_fail_json.saveAsSequenceFile(failOutput)
        elif outputFormat == "text":
            rdd_fail_json.saveAsTextFile(failOutput)
        elif outputFormat == "tsv":
            rdd_tsv = rdd_fail_json.map(lambda (k,p): k + "\t" + p[0] + "\t" + p[1])
            rdd_tsv.saveAsTextFile(failOutput)
        else:
            raise RuntimeError("Unrecognized output format: %s" % outputFormat)

def main(argv=None):
    '''this is called if run from command line'''
    # pprint.pprint(sorted(os.listdir(os.getcwd())))
    parser = argparse.ArgumentParser()
    # parser.add_argument('-c','--cdr', default='data/in/cdr/1ht.json')
    # parser.add_argument('-c','--cdr', default='data/in/cdr/1000ht.json')
    # parser.add_argument('-c','--cdr', default='data/in/cdr/250k_ht.json')
    parser.add_argument('-c','--cdr', default='data/in/cdr/fake.json')
    # parser.add_argument('-s','--stanford', default='data/in/stanford/phone_numbers.tsv')
    parser.add_argument('-s','--stanford', default='data/in/stanford/phone_numbers2.tsv')
    parser.add_argument('-g','--success', required=True)
    parser.add_argument('-f','--fail', required=True)
    parser.add_argument('-k','--shift', default=0, type=int)
    parser.add_argument('-u','--uriClass', default='Offer')
    parser.add_argument('-p','--numPartitions', required=False, default=None, type=int,
                        help='minimum initial number of partitions')
    parser.add_argument('-n','--name', required=False, default="", help='Added to name of spark job, for debugging')
    parser.add_argument('-l','--limit', required=False, default=None, type=int)
    parser.add_argument('-v','--verbose', required=False, help='verbose', action='store_true')
    parser.add_argument('-z','--debug', required=False, help='debug', type=int)
    parser.add_argument('-x','--cheat', required=False, help='cheat', action='store_true')
    parser.add_argument('-y','--outputFormat', default='sequence')
    args=parser.parse_args()

    # might become an option
    outputFormat = 'sequence'
    outputFormat = 'text'
    outputFormat = args.outputFormat

    if not args.numPartitions:
        if location == "local":
            args.numPartitions = 3
        elif location == "hdfs":
            args.numPartitions = 50

    sparkName = "prep"
    if args.name:
        sparkName = sparkName + " " + str(args.name)

    sc = SparkContext(appName=sparkName)
    prep(sc, args.cdr, args.stanford,
         args.success,
         args.fail,
         uriClass=args.uriClass,
         numPartitions=args.numPartitions,
         limit=args.limit,
         debug=args.debug,
         outputFormat=outputFormat,
         shift=args.shift,
         location=location,
         cheat=args.cheat)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

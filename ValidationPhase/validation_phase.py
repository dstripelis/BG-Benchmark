import sys, io, os
from pyspark import SparkConf, SparkContext
from intervaltree import Interval, IntervalTree

APP_NAME = "Log File Handler"

def extendList(a, b):
	c = []
	c.extend(a)
	c.extend(b)
	return c

def isPrimaryKeyTypeUser(keyType):
	if 'user' in keyType:
		return True
	return False

def isPrimaryKeyTypeResource(keyType):
	if 'resource' in keyType:
		return True
	return False

def prepareLogDictionaryFromLogLine(logLine):
	logLine = logLine.strip()
	logDetails = logLine.split(',')
	logRecord = {}
	logRecord['logType'] = logDetails[0]
	logRecord['bgAction'] = logDetails[1]
	logRecord['seqID'] = logDetails[2]
	logRecord['threadID'] = logDetails[3]
	logRecord['primaryKey'] = logDetails[4]
	logRecord['startTimeNS'] = logDetails[5]
	logRecord['endTimeNS'] = logDetails[6]
	logRecord['resultSize'] = logDetails[7]
	if len(logDetails) == 9:
		logRecord['insertOrDelete'] = 'N'
		logRecord['actionType'] = logDetails[8]
	else:
		logRecord['insertOrDelete'] = logDetails[8]
		logRecord['actionType'] = logDetails[9]
	if logRecord['bgAction'] == 'POSTCOMMENT':
		logRecord['primaryKeyType'] = 'resourceID'
	else:
		logRecord['primaryKeyType'] = 'userID'
	return ((logRecord['primaryKey'], logRecord['primaryKeyType']), [logRecord])

def createIntervalDict(logs):
	intervalDict = {}
	for logRecord in logs:
		startTimeNS = logRecord['startTimeNS']
		endTimeNS = logRecord['endTimeNS']
		if (startTimeNS, endTimeNS) not in intervalDict:
			intervalDict[(startTimeNS, endTimeNS)] = []
		intervalDict[(startTimeNS, endTimeNS)].append(logRecord)
	return intervalDict

def populateIntervalTree(logs):
	logIntervalTree = IntervalTree()
	intervalDict = createIntervalDict(logs)
	for interval in intervalDict:
		logRecords = intervalDict[interval]
		startTimeNS = interval[0]
		endTimeNS = interval[1]
		logIntervalTree[startTimeNS:endTimeNS] = logRecords
	return logIntervalTree

class LogFileHandler:

	def initSpark(self):
		conf = SparkConf().setAppName(APP_NAME)
		conf = conf.setMaster("local[*]")
		conf = conf.set("spark.executor.memory", "2g")
		self.sc = SparkContext(conf=conf)
		logger = self.sc._jvm.org.apache.log4j
		logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
		logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )

	def __init__(self, logFileDir):
		self.logFileDir = logFileDir
		self.initSpark()

	def readLogFiles(self):
		print("Generating Log files RDD")
		completeRDD = None
		tmpRDDs = []
		files = os.listdir(self.logFileDir)
		for fileName in files:
			if 'read' not in fileName and 'update' not in fileName:
				continue
			inputFilePath = self.logFileDir + "/" + fileName
			textRDD = self.sc.textFile(inputFilePath)
			tmpRDD = textRDD.map(lambda x: prepareLogDictionaryFromLogLine(x))
			tmpRDDs.append(tmpRDD)
			print fileName
		print("Combining individual RDDs to form a single RDD")
		completeRDD = self.sc.union(tmpRDDs)
		print("Splitting RDD by data item (primary key)")
		completeRDD = completeRDD.reduceByKey(extendList)
		print("Total data points in the logs: " + str(len(completeRDD.collect())))
		return completeRDD

	def createLogIntervalTree(self):
		logsRDD = self.readLogFiles()
		print("Generating interval tree from splitted logs RDD")
		intervalTreeByDataItemRDD = logsRDD.map(lambda x: (x[0], populateIntervalTree(x[1])))
		dataset =  intervalTreeByDataItemRDD.take(1)
		return intervalTreeByDataItemRDD

def main():
	logFileHandler = LogFileHandler('LogRecords')
	logFileHandler.createLogIntervalTree()

if __name__ == '__main__':
	main()

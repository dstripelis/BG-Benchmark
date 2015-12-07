import sys, io, os
from pyspark import SparkConf, SparkContext
from intervaltree import Interval, IntervalTree
from operator import add

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
	logRecord['threadID'] = int(logDetails[3])
	logRecord['primaryKey'] = int(logDetails[4])
	logRecord['startTimeNS'] = long(logDetails[5])
	logRecord['endTimeNS'] = long(logDetails[6])
	logRecord['resultSize'] = int(logDetails[7])
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


def createIntervalDict(logs, bgAction):
	intervalDict = {}
	for log in logs:
		if bgAction not in log['bgAction']:
			continue
		startTimeNS = log['startTimeNS']
		endTimeNS = log['endTimeNS']
		if (startTimeNS, endTimeNS) not in intervalDict:
			intervalDict[(startTimeNS, endTimeNS)] = []
		intervalDict[(startTimeNS, endTimeNS)].append(log)
	return intervalDict


def populateIntervalTree(logs, bgAction, databaseStartState):
	logIntervalTree = IntervalTree()
	currentValues = [databaseStartState]
	if logs is None:
		return logIntervalTree
	intervalDict = createIntervalDict(logs, bgAction)
	for interval in intervalDict:
		startTimeNS = interval[0]
		endTimeNS = interval[1]
		logIntervalTree[startTimeNS:endTimeNS] = intervalDict[interval]
	sortedIntervals = sorted(logIntervalTree)
	logIntervalTreeWithValues = IntervalTree()
	for i in sortedIntervals:
		logsWithValues = []
		currentValuesNext = []
		for log in i.data:
			data = log
			values = []
			for currentValue in currentValues:
				modificationValue = data['resultSize']
				if data['insertOrDelete'] == 'I':
					tmp = currentValue + 1
				else:
					tmp = currentValue - 1
				if tmp not in currentValuesNext:
					if tmp < 0:
						currentValuesNext.append(0)
					else:
						currentValuesNext.append(tmp)
				if tmp not in values:
					if tmp < 0:
						values.append(0)
					else:
						values.append(tmp)
			data['value'] = values
			logsWithValues.append(data)
		currentValues = currentValuesNext
		logIntervalTreeWithValues[i.begin:i.end] = logsWithValues
	return logIntervalTreeWithValues

def populateIntervalTreesForAllBGActions(logs, bgActions={'ACCEPTFRND' : 10, 'PENDFRND' : 0}):
	intervalTrees = {}
	for bgAction in bgActions:
		intervalTrees[bgAction] = populateIntervalTree(logs, bgAction, bgActions[bgAction])
	return intervalTrees

def findOverlappingIntervals(readQuery, intervalTrees):
	intervalTree = intervalTrees[readQuery['bgAction']]
	readStartTime = readQuery['startTimeNS']
	readEndTime = readQuery['endTimeNS']
	bgAction = readQuery['bgAction']
	overlappingIntervals = intervalTree[readStartTime:readEndTime]
	return overlappingIntervals


#get interval tree from 0 to start(readQuery) - 1 and sort in reverse order
#iterate till you find log record with accepted friend as the bg action and set that as the initialState
#if none found, set 10 as the initial state
def findInitialStateForReadQuery(readQuery, intervalTrees, bgActions={'ACCEPTFRND' : 10, 'PENDFRND' : 0}):
	bgAction = readQuery['bgAction']
	intervalTree = intervalTrees[bgAction]
	readQueryStart = readQuery['startTimeNS']
	intervals = intervalTree[1L:readQueryStart - 1L]
	endTimes = {}
	for interval in intervals:
		endTime = interval.end
		logs = interval.data
		if endTime >= readQueryStart:
			continue
		if endTime not in endTimes:
			endTimes[endTime] = []
		endTimes[endTime].append(interval)
	if len(endTimes) == 0:
		initialState = [bgActions[bgAction]]
		return initialState
	kvPair = [(key,value) for (key, value) in sorted(endTimes.items(), reverse=True)][0]
	lastIntervals = kvPair[1]
	#if len(lastIntervals) == 0:
	#	initialState = [bgActions[bgAction]]
	#	return initialState
	initialState = []
	for interval in lastIntervals:
		for log in interval.data:
			initialState.extend(log['value'])
		overlappingIntervals = intervalTree[interval.begin:interval.end]
		for overlappingInterval in overlappingIntervals:
			if interval != overlappingInterval:
				for log in overlappingInterval.data:
					initialState.extend(log['value'])
	return initialState

def getReadLogsForValidation(readLogs):
	validReadLogs = []
	for readLog in readLogs:
		if "ACCEPTFRND" in readLog['bgAction'] or "PENDFRND" in readLog['bgAction']:
			validReadLogs.append(readLog)
	return validReadLogs

def getRangeOfPossibleValues(queryObject):
	initialState = queryObject["initialState"]
	readLog = queryObject["readLog"]
	intervalTree = queryObject["intervalTrees"][readLog['bgAction']]
	overlappingUpdateIntervals = queryObject["overlappingUpdateIntervals"]
	possibleValueRange = []
	possibleValueRange.extend(initialState)
	for overlappingInterval in overlappingUpdateIntervals:
		for log in overlappingInterval.data:
			possibleValueRange.extend(log['value'])
	return {"valueRead" : readLog["resultSize"], "possibleValueRange" : possibleValueRange}

def getValidityOfReadLog(possibleValueRangeObject):
	if possibleValueRangeObject['valueRead'] in possibleValueRangeObject['possibleValueRange']:
		possibleValueRangeObject['validity'] = True
	else:
		#print (str(possibleValueRangeObject) + "  ----------> FALSE")
		possibleValueRangeObject['validity'] = False
	return possibleValueRangeObject

def getValidRecords(allRecords):
	validRecords = []
	for record in allRecords:
		if record["validity"] == True:
			validRecords.append(record)
	return validRecords


class LogFileHandler:


	def initSpark(self):
		conf = SparkConf().setAppName(APP_NAME)
		conf = conf.setMaster("local[*]")
		self.sc = SparkContext(conf=conf)
		logger = self.sc._jvm.org.apache.log4j
		logger.LogManager.getLogger("org").setLevel( logger.Level.OFF )
		logger.LogManager.getLogger("akka").setLevel( logger.Level.OFF )

	def __init__(self, logFileDir):
		self.logFileDir = logFileDir
		self.initSpark()

	def readLogFiles(self):
		print("Generating Log files RDD for individual log files")
		completeRDD = None
		readTmpRDDs = []
		updateTmpRDDs = []
		files = os.listdir(self.logFileDir)
		for fileName in files:
			if 'read' not in fileName and 'update' not in fileName:
				continue
			inputFilePath = self.logFileDir + "/" + fileName
			textRDD = self.sc.textFile(inputFilePath)
			tmpRDD = textRDD.map(lambda x: prepareLogDictionaryFromLogLine(x))
			if 'read' in fileName:
				readTmpRDDs.append(tmpRDD)
			elif 'update' in fileName:
				updateTmpRDDs.append(tmpRDD)
		print("Combining individual RDDs to form a single RDD")
		readCompleteRDD = self.sc.union(readTmpRDDs)
		updateCompleteRDD = self.sc.union(updateTmpRDDs)
		print("Splitting RDD by data item (primary key)")
		readCompleteRDD = readCompleteRDD.reduceByKey(extendList)
		updateCompleteRDD = updateCompleteRDD.reduceByKey(extendList)
		completeRDD = readCompleteRDD.leftOuterJoin(updateCompleteRDD)
		return completeRDD

	def createLogIntervalTree(self):
		logsRDD = self.readLogFiles()
		print("Generating interval tree from splitted logs RDD")
		intervalTreeByDataItemRDD = logsRDD.map(lambda x: (x[0],{"intervalTrees" : populateIntervalTreesForAllBGActions(x[1][1]), "readLogs" : x[1][0]}))
		return intervalTreeByDataItemRDD

	def findOverlappingUpdateLogsAndInitialStateOfEveryReadLog(self):
		intervalTreeRDD = self.createLogIntervalTree()
		print("Finding overlapping update logs for each read log for every data item")#find database initial state corresponding to each read query
		validReadLogsRDD = intervalTreeRDD.map(lambda x : (x[0], {"intervalTrees" : x[1]["intervalTrees"] ,"readLogs" : getReadLogsForValidation(x[1]["readLogs"])}))
		overlappingUpdateLogsWithInitialStateRDD = intervalTreeRDD.map(lambda x : (x[0], [{"intervalTrees" : x[1]["intervalTrees"] ,"readLog" : readQuery, "overlappingUpdateIntervals" : findOverlappingIntervals(readQuery, x[1]["intervalTrees"]), "initialState" : findInitialStateForReadQuery(readQuery, x[1]["intervalTrees"])} for readQuery in x[1]["readLogs"]]))
		return overlappingUpdateLogsWithInitialStateRDD

	def findPossibleValueRangeForReadLogs(self):
		queryRDD = self.findOverlappingUpdateLogsAndInitialStateOfEveryReadLog()
		print("Finding possible range of valid values for each read log based on interval tree for every data item")
		possibleValueRangeRDD = queryRDD.map(lambda x : (x[0], [getRangeOfPossibleValues(queryObject) for queryObject in x[1]]))
		return possibleValueRangeRDD

	def validateReadLogs(self):
		possibleValueRangeRDD = self.findPossibleValueRangeForReadLogs()
		print("Validating each read log for every data item")
		validationRDD = possibleValueRangeRDD.map(lambda x : (x[0], [getValidityOfReadLog(possibleValueRangeObject) for possibleValueRangeObject in x[1]]))
		return validationRDD

	def getOverallValidData(self):
		validationRDD = self.validateReadLogs()
		print("Calculating percentage of valid data...")
		totalValidCount = validationRDD.map(lambda x : ("count of valid read logs", len(getValidRecords(x[1])))).reduceByKey(add).collect()
		print totalValidCount
		totalReadCount = validationRDD.map(lambda x : ("count of total read logs", len(x[1]))).reduceByKey(add).collect()
		print totalReadCount

		validationPercentage = float(totalValidCount[0][1])/float(totalReadCount[0][1]) * 100.0
		print("Total percentage of valid data = " + str(validationPercentage))

def main():
	logFileHandler = LogFileHandler('logs2')
	logFileHandler.getOverallValidData()

if __name__ == '__main__':
	main()

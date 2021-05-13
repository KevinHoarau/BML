#!/usr/bin/env python3

import sys, os, time
from pybgpstream import BGPStream, BGPRecord, BGPElem
from threading import Thread
import ipaddress as ip
import json
from BML.utils import utils

def serialize_sets(obj):
    if isinstance(obj, set):
        return list(obj)

    return obj

class UpdatesDump(Thread):

	def __init__(self, path):

		Thread.__init__(self)

		self.startTime = -1
		self.endTime = -1

		self.projects = ['ris','routeviews']
		self.collectors = []
		self.ipVersion = [4,6]
		self.useRibs = False

		self.stream = BGPStream()

		self.currentTime = -1
		self.finished = False
		self.terminate = False

		self.filePath = self.getFilePath(path)

	def setUseRibs(self, useRibs):
		self.useRibs = useRibs

	def setProjects(self, projects):
		self.projects = projects


	def setIpVersion(self, ipVersion):
		self.ipVersion = ipVersion

	def setCollectors(self, collectors):
		self.collectors = collectors

	def setInterval(self, start, end):
		self.startTime = start
		self.endTime = end

	def startStream(self):

		if(self.startTime!=-1 and self.endTime!=-1):

			if(self.useRibs):
				self.stream = BGPStream(
					from_time=self.startTime, until_time=self.endTime,
					collectors=self.collectors,
					projects=self.projects
					)
			else:
				self.stream = BGPStream(
					from_time=self.startTime, until_time=self.endTime,
					collectors=self.collectors,
					record_type="updates",
					projects=self.projects
					)

		else:
			quit("Error: can't start stream, interval not set")

	def getProgress(self):
		progress = 0
		if(self.currentTime!=-1):
			progress = (self.currentTime-self.startTime)*100//(self.endTime-self.startTime)
		return(progress)
		
	def isRunning(self):
		return(not self.finished)

	def stop(self):
		self.terminate = True


	def buildUpdatesDump(self):

		self.emptyFile()

		for record in self.stream.records():

			if(self.terminate):
				break

			if record.status == "valid":


				for elem in record:

					self.currentTime = elem.time

					if(elem.type=='A' or elem.type=='W' or elem.type=='R'):

						u = {}

						u['collector'] = str(record.collector)
						u['dump_time'] = str(record.dump_time)
						u['type'] = str(elem.type)
						u['time'] = str(int(elem.time))
						u['peer_address'] = str(elem.peer_address)
						u['peer_asn'] = str(elem.peer_asn)
						u['fields'] = json.dumps(elem.fields, default=serialize_sets)
						
						try:
							network = ip.ip_network(elem.fields['prefix'])
							if(network.version in self.ipVersion):
								self.appendToFile(u)
						except Exception as e:
							print(e)

		self.finished = True


	def getOutputFilename(self):
		return("updates.csv")


	def getFilePath(self, path):
		return(utils.mkdirPath(path) + self.getOutputFilename())

	def emptyFile(self):

		file = open(self.filePath,"w")
		file.write("collector,dump_time,type,time,peer_address,peer_asn,fields" + '\n')
		file.close()

		return(self.filePath)

	def appendToFile(self, u):

		file = open(self.filePath,"a")
		file.write(u['collector']+","+u['dump_time']+","+u['type']+","+u['time']+","+u['peer_address']+","+u['peer_asn']+","+u['fields'] + '\n')
		file.close()

		return(self.filePath)


	def run(self):

		self.startStream()
		self.buildUpdatesDump()


def dumpUpdates(start, end, folder, collectors, logFiles=[], projects=["ris"], ipVersion=[4,6], useRibs=False):

	logFile = open(utils.mkdirPath(folder)+"log_updates_dump.log",'w')
	logFiles.append(logFile)

	updatesDump = UpdatesDump(folder)

	try:

		updatesDump.setInterval(start,end)
		updatesDump.setProjects(projects)
		updatesDump.setCollectors(collectors)
		updatesDump.setIpVersion(ipVersion)
		updatesDump.setUseRibs(useRibs)
		
		utils.printAndLog("###############", logFiles)
		utils.printAndLog("# Updates dump", logFiles)
		utils.printAndLog("###############", logFiles)
		utils.printAndLog("Start time: " + str(updatesDump.startTime), logFiles)
		utils.printAndLog("End time: " + str(updatesDump.endTime), logFiles)
		utils.printAndLog("Duration: " + utils.timeFormat(updatesDump.endTime-updatesDump.startTime), logFiles)
		utils.printAndLog("Projects: " + str(updatesDump.projects), logFiles)
		utils.printAndLog("Collectors: " + str(updatesDump.collectors), logFiles)
		utils.printAndLog("Ip version: " + str(updatesDump.ipVersion), logFiles)

		timeAtStart = time.time() 

		updatesDump.start()
		
		utils.printProgress(updatesDump, logFiles)

		utils.printAndLog("Computation time: " + utils.timeFormat(time.time()-timeAtStart), logFiles)
		utils.printAndLog("Updates dump saved to: " + updatesDump.filePath, logFiles)

	except KeyboardInterrupt:
		utils.printAndLog('User interrupted, ask updates dump to stop...', logFiles)
		updatesDump.stop()
		quit()

	logFile.close()

	return(updatesDump.filePath)

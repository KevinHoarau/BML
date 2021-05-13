#!/usr/bin/env python3

import sys, os, time
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from threading import Thread
import ipaddress as ip
import json
from BML.utils import utils

class RoutesDump(Thread):

	def __init__(self):

		Thread.__init__(self)

		self.startTime = -1
		self.endTime = -1

		self.projects = ['ris','routeviews']
		self.collectors = []
		self.ipVersion = [4,6]

		self.stream = BGPStream()

		self.routes = {}
		self.currentTime = -1
		self.finished = False
		self.terminate = False

		self.updates = None
		self.updatesFilePath = None

	def setProjects(self, projects):
		self.projects = projects

	def setIpVersion(self, ipVersion):
		self.ipVersion = ipVersion

	def setCollectors(self, collectors):
		self.collectors = collectors

	def setInterval(self, start, end):
		self.startTime = start
		self.endTime = end

	def setUpdates(self, updates):
		self.updates = updates

	def setUpdatesFile(self, filePath):
		self.updatesFilePath = filePath

	def startStream(self):

		if(self.startTime!=-1 and self.endTime!=-1):

			for project in self.projects:
				self.stream.add_filter('project', project)

			if(len(self.collectors)>0):
				for collector in self.collectors:
					self.stream.add_filter('collector', collector)

			self.stream.add_interval_filter(self.startTime,self.endTime)
			self.stream.add_filter('record-type','updates')

			self.stream.start()
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


	def dumpUpdate(self, u):

		self.currentTime = int(u['time'])

		if(u['type']=='A' or u['type']=='W' or u['type']=='R'):

			collector = u['collector']
			peer = u['peer_asn']
			fields = json.loads(u['fields'])
			prefix = fields['prefix']

			network = ip.ip_network(prefix)
			if(network.version in self.ipVersion):

				if(u['type']=='A' or u['type']=='R'):

					if(prefix not in self.routes):
						self.routes[prefix] = {}
					if(collector not in self.routes[prefix]):
						self.routes[prefix][collector] = {}

					self.routes[prefix][collector][peer] = fields['as-path']


				elif(u['type']=='W'):

					if(prefix in self.routes and collector in self.routes[prefix] and  peer in self.routes[prefix][collector]):
						self.routes[prefix][collector][peer] = None


	def buildRoutesDump(self):

		record = BGPRecord()

		while(self.stream.get_next_record(record) and not self.terminate):

			if record.status == "valid":

				elem = record.get_next_elem()

				while(elem and not self.terminate):

					u= {}

					u['collector'] = str(record.collector)
					u['dump_time'] = str(record.dump_time)
					u['type'] = str(elem.type)
					u['time'] = str(elem.time)
					u['peer_address'] = str(elem.peer_address)
					u['peer_asn'] = str(elem.peer_asn)
					u['fields'] = json.dumps(elem.fields)

					self.dumpUpdate(u)
					
					elem = record.get_next_elem()

		self.finished = True

	def buildRoutesDumpFromUpdates(self):

		headerLine = ""
		firstLine = ""
		lastLine = ""
		
		i = 0
		for line in self.updates:
			if(i==0):
				headerLine = line
			if(i==1):
				firstLine = line

			i+= 1
		lastLine = line

		header = {}
		headerSplited = headerLine.split(',')
		nbItems = len(headerSplited)
		for i in range(len(headerSplited)):
			header[headerSplited[i].replace('\n','')] = i

		self.startTime = int(firstLine.split(',')[header['time']])
		self.endTime = int(lastLine.split(',')[header['time']])

		i=0
		for line in self.updates:

			if(i!=0):

				lineSplited = line.split(',')
				lineSplited[nbItems-1] = ','.join(lineSplited[nbItems-1::])

				u= {}

				u['collector'] = lineSplited[header['collector']]
				u['dump_time'] = lineSplited[header['dump_time']]
				u['type'] = lineSplited[header['type']]
				u['time'] = lineSplited[header['time']]
				u['peer_address'] = lineSplited[header['peer_address']]
				u['peer_asn'] = lineSplited[header['peer_asn']]
				u['fields'] = lineSplited[header['fields']]

				self.dumpUpdate(u)

			i+=1
		
		self.finished = True
	

	def buildRoutesDumpFromUpdatesFile(self):

		headerLine = ""
		firstLine = ""
		lastLine = ""
		
		with open(self.updatesFilePath) as file:

			i = 0
			for line in file:
				if(i==0):
					headerLine = line
				if(i==1):
					firstLine = line

				i+= 1


			lastLine = line
			file.close()

		header = {}
		headerSplited = headerLine.split(',')
		nbItems = len(headerSplited)
		for i in range(len(headerSplited)):
			header[headerSplited[i].replace('\n','')] = i

		self.startTime = int(firstLine.split(',')[header['time']])
		self.endTime = int(lastLine.split(',')[header['time']])

		with open(self.updatesFilePath) as file:

			i=0
			for line in file:

				if(i!=0):

					lineSplited = line.split(',')
					lineSplited[nbItems-1] = ','.join(lineSplited[nbItems-1::])

					u= {}

					u['collector'] = lineSplited[header['collector']]
					u['dump_time'] = lineSplited[header['dump_time']]
					u['type'] = lineSplited[header['type']]
					u['time'] = lineSplited[header['time']]
					u['peer_address'] = lineSplited[header['peer_address']]
					u['peer_asn'] = lineSplited[header['peer_asn']]
					u['fields'] = lineSplited[header['fields']]


					self.dumpUpdate(u)

				i+=1
		
		self.finished = True


	def getOutputFilename(self):
		return("routes.json")


	def saveTo(self, path):

		filePath = utils.mkdirPath(path) + self.getOutputFilename()

		file = open(filePath,"w")
		json.dump(self.routes, file)
		file.close()

		return(filePath)


	def run(self):

		if(self.updates!=None):
			self.buildRoutesDumpFromUpdates()
		elif(self.updatesFilePath!=None):
			self.buildRoutesDumpFromUpdatesFile()
		else:
			self.startStream()
			self.buildRoutesDump()


def dumpRoutes(start, end, folder, collectors, routes=None, logFiles=[], saveRoutes=True, updates=None, updatesDumpFile=None, returnRoutes=False, projects=["ris"], ipVersion=[4,6]):

	logFile = open(utils.mkdirPath(folder)+"log_routes_dump.log",'w')
	logFiles.append(logFile)

	routesDump = RoutesDump()

	if(routes):
		routesDump.routes = routes

	try:

		routesDump.setInterval(start,end)
		routesDump.setProjects(projects)
		routesDump.setCollectors(collectors)
		routesDump.setUpdates(updates)
		routesDump.setUpdatesFile(updatesDumpFile)
		routesDump.setIpVersion(ipVersion)
		
		utils.printAndLog("#############", logFiles)
		utils.printAndLog("# Route dump", logFiles)
		utils.printAndLog("#############", logFiles)

		if(updates!=None):
			utils.printAndLog("Nb. of Updates: " + str(len(routesDump.updates)), logFiles)
		elif(updatesDumpFile!=None):
			utils.printAndLog("Updates file: " + str(routesDump.updatesFilePath), logFiles)
		else:
			utils.printAndLog("Start time: " + str(routesDump.startTime), logFiles)
			utils.printAndLog("End time: " + str(routesDump.endTime), logFiles)
			utils.printAndLog("Duration: " + utils.timeFormat(routesDump.endTime-routesDump.startTime), logFiles)
			utils.printAndLog("Projects: " + str(routesDump.projects), logFiles)
			utils.printAndLog("Collectors: " + str(routesDump.collectors), logFiles)
		utils.printAndLog("Ip version: " + str(routesDump.ipVersion), logFiles)

		timeAtStart = time.time() 

		routesDump.start()
		
		utils.printProgress(routesDump, logFiles)

		utils.printAndLog("Computation time: " + utils.timeFormat(time.time()-timeAtStart), logFiles)
		utils.printAndLog("Nb. of prefix in route dump: " + str(len(routesDump.routes.keys())), logFiles)

		if(saveRoutes):
			filePath = routesDump.saveTo(folder)
			utils.printAndLog("Route dump saved to: " + filePath, logFiles)

	except KeyboardInterrupt:
		utils.printAndLog('User interrupted, ask route dump to stop...', logFiles)
		routesDump.stop()
		quit()

	logFile.close()

	if(returnRoutes):
		return(routesDump.routes)
	else:
		return(filePath)
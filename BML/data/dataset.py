#!/usr/bin/env python3

import os, random
import json
from BML.utils import utils
from BML.data.updates import dumpUpdates
from BML.data.routes import dumpRoutes

def processSample(label, start_time, end_time, name, priming, collectors, folder, ipVersion, projects, useRibsPriming, useRibsData, skipIfExist, logFiles=[]):

	e_folder = utils.mkdirPath(folder + label + os.sep + str(name))
	logFiles.append(open(e_folder + "log_collect_sample.log", "w"))

	utils.printAndLog("##################", logFiles)
	utils.printAndLog("# Collect sample", logFiles)
	utils.printAndLog("#################", logFiles)

	utils.printAndLog("Name: {}".format(name), logFiles)
	utils.printAndLog("Label: {}".format(label), logFiles)
	utils.printAndLog("Start time: {}".format(start_time), logFiles)
	utils.printAndLog("End time: {}".format(end_time), logFiles)

	utils.printAndLog("**************************", logFiles)
	utils.printAndLog("* Priming data collection", logFiles)
	utils.printAndLog("**************************", logFiles)

	if(os.path.exists(e_folder + "priming_data" + os.sep +"routes.json.gz") and skipIfExist):
		utils.printAndLog("Data exists, skipped", logFiles)
	else:
		# Priming data collection
		primingUpdatesFile = dumpUpdates(start_time-priming, start_time, e_folder + "priming_data", collectors, logFiles=logFiles[:], projects=projects, ipVersion=ipVersion, useRibs=useRibsPriming)
		primingDumpFile = dumpRoutes(0, 0, e_folder + "priming_data", [], saveRoutes=True, updatesDumpFile=primingUpdatesFile, logFiles=logFiles[:], projects=projects, ipVersion=ipVersion)

		utils.gzipFile(primingDumpFile, remove=True)
		os.remove(primingUpdatesFile)

	utils.printAndLog("********************", logFiles)
	utils.printAndLog("* Data collection", logFiles)
	utils.printAndLog("********************", logFiles)

	if(os.path.exists(e_folder + "data" + os.sep +"updates.csv.gz") and skipIfExist):
		utils.printAndLog("Data exists, skipped", logFiles)
	else:
		# Data collection
		updatesFilePath = dumpUpdates(start_time, end_time, e_folder + "data", collectors, logFiles=logFiles[:], projects=projects, ipVersion=ipVersion, useRibs=useRibsData)
		
		utils.gzipFile(updatesFilePath, remove=True)

class Dataset():

	def __init__(self, folder):

		self.projects = ['ris','routeviews']
		self.collectors = []
		self.ipVersion = [4,6]
		self.periodsOfInterests = []
		self.folder = folder
		self.primingPeriod = 1*60*60
		self.skipIfExist = True
		self.useRibsPriming = False
		self.useRibsData = False

	def setUseRibs(self, useRibsPriming, useRibsData):
		self.useRibsPriming = useRibsPriming
		self.useRibsData = useRibsData

	def setIpVersion(self, ipVersion):
		self.ipVersion = ipVersion

	def setProjects(self, projects):
		self.projects = projects

	def setCollectors(self, collectors):
		self.collectors = collectors

	def setPeriodsOfInterests(self, periodsOfInterests):
		self.periodsOfInterests = periodsOfInterests

	def setPrimingPeriod(self, primingPeriod):
		self.primingPeriod = primingPeriod * 60

	def setFolder(self, folder):
		self.folder = folder

	def setSkipIfExist(self, skipIfExist):
		self.skipIfExist = skipIfExist

	def load(self, d):
		if("IpVersion" in d.keys()):
			self.setIpVersion(d["IpVersion"])
		
		if("Projects" in d.keys()):
			self.setProjects(d["Projects"])
		
		if("Collectors" in d.keys()):
			self.setCollectors(d["Collectors"])
		
		if("PeriodsOfInterests" in d.keys()):
			self.setPeriodsOfInterests(d["PeriodsOfInterests"])
		
		if("PrimingPeriod" in d.keys()):
			self.setPrimingPeriod(d["PrimingPeriod"])

	def loadFromFile(self, fpath):
		self.load(json.load(open(fpath)))

	def getJobs(self):

		jobs = []

		for period in self.periodsOfInterests:

			j = {
				'includes' : "from BML.data.dataset import processSample",
				'target': processSample.__name__,
				'args': (period["label"], period["start_time"], period["end_time"], period["name"], self.primingPeriod, self.collectors, self.folder, self.ipVersion, self.projects, self.useRibsPriming, self.useRibsData, self.skipIfExist),
				'kwargs': {'logFiles':["LOG_ONLY"]}
			}

			jobs.append(j)


		random.shuffle(jobs) 

		return(jobs)

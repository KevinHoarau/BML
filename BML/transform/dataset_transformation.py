#!/usr/bin/env python3

import os, random, time
import json
from BML.utils import utils
from BML.data.updates import dumpUpdates
from BML.data.routes import dumpRoutes

def getFolders(folder):

	folders = []

	for r, d, f in os.walk(folder):
		for filename in f:

			dirname = os.path.dirname(r)

			if("routes.json" in filename and not dirname in folders):
				folders.append(dirname)

	return(folders)

def processSample(folder, transform_module, transform_name, params, skipIfExist,logFiles=[]):
	
	outputfolder = utils.mkdirPath(folder + os.sep + "transform" + os.sep + transform_name)
	logFiles.append(open(outputfolder + os.sep + "log_transform_sample.log", "w"))

	utils.printAndLog("###################", logFiles)
	utils.printAndLog("# Transform sample", logFiles)
	utils.printAndLog("###################", logFiles)

	utils.printAndLog("Folder: {}".format(folder), logFiles)
	utils.printAndLog("Transform: {}".format(transform_name), logFiles)
	utils.printAndLog("Params: \n{}".format(params), logFiles)


	utils.printAndLog("##################", logFiles)
	utils.printAndLog("# Transform", logFiles)
	utils.printAndLog("#################", logFiles)

	exec("from " + transform_module + " import " + transform_name + " as Transforms")

	transform = locals()["Transforms"]()
	transform.setLogFiles(logFiles)
	transform.setFolder(folder)
	transform.setOutputFolder(outputfolder)
	transform.setParams(params)

	if(transform.exists() and skipIfExist):
		utils.printAndLog("Data exists, skipped", logFiles)
	else:
		timeAtStart = time.time() 
		transform.start()

		utils.printProgress(transform, logFiles)

		utils.printAndLog("Computation time: " + utils.timeFormat(time.time()-timeAtStart), logFiles)

		transform.save(gzip=False)


class DatasetTransformation():

	def __init__(self, folder, transform_module, transform_name):

		self.folder = folder
		self.transform_module = transform_module
		self.transform_name = transform_name
		self.skipIfExist = True
		self.params = None
		self.folders = None

	def setParams(self, params):
		self.params = params

	def setFolders(self, folders):
		self.folders = folders

	def setSkipIfExist(self, skipIfExist):
		self.skipIfExist = skipIfExist

	def getJobs(self):

		jobs = []

		if(self.folders is None):
			folders = getFolders(self.folder)
		else:
			folders = self.folders

		for f in folders:

			if("global" in self.params):
				params = self.params["global"].copy()
			else:
				params = {}

			path = f.split(os.sep)
			label = path[-2]
			name = path[-1]

			if(label in self.params and name in self.params[label]):
				for key, value in self.params[label][name].items():
					params[key] = value

			j = {
				'includes' : "from BML.transform.dataset_transformation import processSample",
				'target': processSample.__name__,
				'args': (f, self.transform_module, self.transform_name, params, self.skipIfExist),
				'kwargs': {'logFiles':["LOG_ONLY"]}
			}

			jobs.append(j)


		#random.shuffle(jobs) 

		return(jobs)

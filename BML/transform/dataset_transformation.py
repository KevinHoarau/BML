#!/usr/bin/env python3

import os, random, time
import json
from BML.utils import utils
from BML.transform.base_transform import transform

def getFolders(folder):

    folders = []

    for r, d, f in os.walk(folder):
        for filename in f:

            dirname = os.path.dirname(r)

            if("routes.json" in filename and not dirname in folders):
                folders.append(dirname)

    return(folders)

def transformSample(folder, transform_module, transform_name, params, logFiles=[]):
    
    outputfolder = utils.mkdirPath(folder + os.sep + "transform" + os.sep + transform_name)
    
    primingFile = folder + os.sep + "priming_data" + os.sep + "routes.json.gz"
    dataFile = folder + os.sep + "data" + os.sep + "updates.csv.gz"
    
    exec("from " + transform_module + " import " + transform_name + " as Transforms")
    transformation = locals()["Transforms"]
    transform(transformation, primingFile, dataFile, params=params, outFolder=outputfolder, logFiles=logFiles)

class DatasetTransformation():

    def __init__(self, folder, transform_module, transform_name):

        self.folder = folder
        self.transform_module = transform_module
        self.transform_name = transform_name
        self.folders = None

    def setParams(self, params):
        self.params = params

    def setFolders(self, folders):
        self.folders = folders

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
                'includes' : "from BML.transform.dataset_transformation import transformSample",
                'target': transformSample.__name__,
                'args': (f, self.transform_module, self.transform_name, params),
                'kwargs': {'logFiles':["LOG_ONLY"]}
            }

            jobs.append(j)

        random.shuffle(jobs) 

        return(jobs)

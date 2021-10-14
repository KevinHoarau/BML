#!/usr/bin/env python3

import sys, os, random
import json
from BML import utils
from BML.data.updates import dumpUpdates
from BML.data.routes import dumpRoutes

def collectData(label, start_time, end_time, name, folder, params, logFiles=[]):

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

    if(os.path.exists(e_folder + "priming_data" + os.sep +"routes.json.gz") and params["SkipIfExist"]):
        utils.printAndLog("Data exists, skipped", logFiles)
    else:
        
        # Priming data collection
        paramsUpdate = {
            "Projects": params["Projects"],
            "Collectors": params["Collectors"],
            "IpVersion": params["IpVersion"],
            "UseRibs": params["UseRibsPriming"],
        }
        primingUpdatesFile = dumpUpdates(start_time-params["PrimingPeriod"]*60, start_time, e_folder + "priming_data", params=paramsUpdate, logFiles=logFiles[:])
        _ , primingDumpFile = dumpRoutes(primingUpdatesFile, routes={}, outFolder=e_folder + "priming_data", logFiles=logFiles[:])

        utils.gzipFile(primingDumpFile, remove=True)
        os.remove(primingUpdatesFile)

    utils.printAndLog("********************", logFiles)
    utils.printAndLog("* Data collection", logFiles)
    utils.printAndLog("********************", logFiles)

    if(os.path.exists(e_folder + "data" + os.sep +"updates.csv.gz") and params["SkipIfExist"]):
        utils.printAndLog("Data exists, skipped", logFiles)
    else:
        # Data collection
        paramsUpdate = {
            "Projects": params["Projects"],
            "Collectors": params["Collectors"],
            "IpVersion": params["IpVersion"],
            "UseRibs": params["UseRibsData"],
        }
        updatesFilePath = dumpUpdates(start_time, end_time, e_folder + "data", params=paramsUpdate, logFiles=logFiles[:])
        
        utils.gzipFile(updatesFilePath, remove=True)

class Dataset():

    def __init__(self, folder):
        
        self.params = {
            "Projects" : ['ris','routeviews'],
            "Collectors" : [],
            "IpVersion" : [4,6],
            "PrimingPeriod" : 1*60,
            "UseRibsPriming" : False,
            "UseRibsData" : False,
            "SkipIfExist" : True
        }
        
        self.folder= folder
    
    def setParams(self, params):
        for k,v in params.items():
            if(k in self.params):
                self.params[k] = v
            else:
                sys.exit("Unrecognized parameter:"+k)
                
    def setPeriodsOfInterests(self, periodsOfInterests):
        self.periodsOfInterests = periodsOfInterests

    def getJobs(self):

        jobs = []

        for period in self.periodsOfInterests:
            
            params = self.params.copy()
            
            if("params" in period):
                for k,v in period["params"].items():
                    if(k in params):
                        params[k] = v
                    else:
                        sys.exit("Unrecognized parameter:"+k)
                
            j = {
                'includes' : "from BML.data.dataset import collectData",
                'target': "collectData",
                'args': (period["label"], period["start_time"], period["end_time"], period["name"], self.folder, params),
                'kwargs': {'logFiles':["LOG_ONLY"]}
            }

            jobs.append(j)

        random.shuffle(jobs) 

        return(jobs)

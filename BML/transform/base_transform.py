 #!/usr/bin/env python3

import os, time
from threading import Thread
import ujson as json
from BML.utils import utils
from BML.data.routes import dumpRoutes
from BML.utils import ProcessingQueue
import multiprocessing

from math import ceil
import ipaddress as ip

import copy

from sys import getsizeof

class BaseTransform(Thread):

    computeRoutes = True
    loadPrimingData = True

    def __init__(self):

        Thread.__init__(self)

        self.folder = ""
        self.outputFolder = ""

        self.current = -1
        self.end = 0
        self.finished = False

        self.logFiles = []

        self.params = {
            "Name": None,
            "Period": 5,
            "Collectors": [],
            "IpVersion": [4,6]
        }

    def setParams(self, params):
        for k,v in params.items():
            self.params[k] = v

    def setFolder(self, folder):
        self.folder = folder

    def setOutputFolder(self, outputFolder):
        self.outputFolder = outputFolder
 
    def setLogFiles(self, logFiles):
        self.logFiles = logFiles

    def getProgress(self):
        progress = 0
        if(self.current!=-1):
            progress = (self.current - self.startTime)*100//(self.endTime - self.startTime)
        return(progress)

    def isRunning(self):
        return(not self.finished)

    def stop(self):
        self.terminate()

    def prepare(self):

        utils.printAndLog("Prepare", self.logFiles)

        priming_data_filepath = self.folder + os.sep + "priming_data" + os.sep + "routes.json"
        updates_filepath = self.folder + os.sep + "data" + os.sep + "updates.csv"

        if(self.loadPrimingData):
            utils.printAndLog("Ungzip priming data", self.logFiles)
            utils.ungzipFile(priming_data_filepath + ".gz")

        utils.printAndLog("Ungzip updates", self.logFiles)
        utils.ungzipFile(updates_filepath + ".gz")

        self.priming_data_filepath = priming_data_filepath
        self.updates_filepath = updates_filepath

        utils.printAndLog("Load priming data", self.logFiles)

        self.prior_routes = {}
        if(self.loadPrimingData):
            self.prior_routes = json.load(open(priming_data_filepath))
            for prefix in list(self.prior_routes.keys()):

                network = ip.ip_network(prefix)

                if(network.version in self.params["IpVersion"]):
        
                    if(len(self.params["Collectors"])>0):

                        for collector in list(self.prior_routes[prefix].keys()):
                            if(not collector in self.params["Collectors"]):
                                self.prior_routes[prefix].pop(collector)
                else:
                    self.prior_routes.pop(prefix)

    def init(self):

        utils.printAndLog("Init", self.logFiles)

        # Update Data
        updates_file = open(self.updates_filepath)
        c = 0

        for line in updates_file:
            update = line[:-1].split(',')

            if(c==0):
                updatesHeader = update
                updatesHeaderIndex = utils.getIndexList(updatesHeader)
            else:

                if(c==1):
                    firstUpdate = update
                else:
                    lastUpdate = update

            c += 1

        self.startTime = int(firstUpdate[updatesHeader.index("time")])
        self.endTime = int(lastUpdate[updatesHeader.index("time")])

        self.T = ceil((self.endTime-self.startTime)/60 /self.params["Period"]) + 1 # +1 because the first snapshot is at t=0

        self.end = c-1


    def computeSnapshot(self, t, routes, updatesParsed):
        self.transformedData.append(self.transforms(t, routes, updatesParsed))

    def compute(self):

        utils.printAndLog("compute", self.logFiles)

        self.preProcess()

        self.transformedData = []

        updates_file = open(self.updates_filepath)

        routes = self.prior_routes
        updates = []
        updatesParsed = []

        c = 0
        t = 0

        for line in updates_file:

            lineSplited = line[:-1].split(',')

            if(c==0):
                header = utils.getIndexList(lineSplited)
                headerLine = line
                updates = [headerLine]
        
            else:

                update = self.parseUpdate(lineSplited, header)

                if(self.startTime + t*self.params["Period"] *60 <= int(update["time"]) and t < self.T):
                    
                    if(t>0 and self.computeRoutes):
                        routes = dumpRoutes(0, 0, self.outputFolder, [], routes=routes, logFiles=["LOG_ONLY"], saveRoutes=False, updates=updates, returnRoutes=True)   
                    
                    self.computeSnapshot(t, routes, updatesParsed)

                    updates = [headerLine]
                    updatesParsed = []

                    t += 1


                network = ip.ip_network(update["fields"]["prefix"])

                if(network.version in self.params["IpVersion"]):
                    if(len(self.params["Collectors"])>0):

                        if(update["collector"] in self.params["Collectors"]):
                            updates.append(line)
                            updatesParsed.append(update)

                    else:
                        updates.append(line)
                        updatesParsed.append(update)

                self.current = int(update["time"])

            c += 1


    def parseUpdate(self, lineSplited, header):

        u= {}

        u['collector'] = lineSplited[header['collector']]
        u['dump_time'] = lineSplited[header['dump_time']]
        u['type'] = lineSplited[header['type']]
        u['time'] = lineSplited[header['time']]
        u['peer_address'] = lineSplited[header['peer_address']]
        u['peer_asn'] = lineSplited[header['peer_asn']]
        u['fields'] = json.loads(",".join(lineSplited[header['fields']::]))

        return(u)


    def getOutputFilename(self):
        if(self.params["Name"] is None):
            return(self.__class__.__name__ + "_" + str(self.params["Period"]))
        else:
            return(self.params["Name"])

    def save(self, gzip=True):

        self.saveData(utils.mkdirPath(self.outputFolder), self.getOutputFilename(), self.transformedData, gzip)

        utils.printAndLog("saved", self.logFiles)

    def exists(self):
        filepath = self.outputFolder + os.sep + self.getOutputFilename() + ".json"
        return(os.path.exists(filepath) or os.path.exists(filepath+".gz"))

    def saveData(self, folder, fileName, data, gzip):

        filePath = folder + fileName + ".json"

        with open(filePath, 'w') as outfile:
            json.dump(data, outfile)

        if(gzip):
            utils.gzipFile(filePath)
            filePath += ".gz"

    def transforms(self, index, routes, updates):
        return(None)

    def preProcess(self):
        return(None)

    def postProcess(self, transformedData):
        return(transformedData)

    def run(self):

        self.prepare()

        self.init()

        self.compute()

        self.transformedData = self.postProcess(self.transformedData)

        if(os.path.exists(self.updates_filepath+".gz")):
            os.remove(self.updates_filepath)
        if(os.path.exists(self.priming_data_filepath+".gz")):
            if(self.loadPrimingData):
                os.remove(self.priming_data_filepath)
        
        self.finished = True


class BaseTransformParallelized(BaseTransform):

    def __init__(self):

        BaseTransform.__init__(self)
        self.params["nbProcess"] = multiprocessing.cpu_count()

    def preProcess(self):
        manager = multiprocessing.Manager()
        self.data = manager.dict()
        self.pq = ProcessingQueue(nbProcess=self.params["nbProcess"])
        return(None)

    def computeSnapshot(self, t, routes, updatesParsed):
        self.pq.waitUntilFree()
        self.pq.addProcess(target=self.runTransforms, args=(self.data, t, routes, updatesParsed))
        self.pq.runOnce()

    def compute(self):

        BaseTransform.compute(self)

        self.pq.run()

        for i in range(len(self.data)):
            if(i in self.data):
                self.transformedData.append(self.data[i])
                #self.transformedData.append(self.data[i].copy())
                del self.data[i]

    def runTransforms(self, data, index, routes, updates):
        data[index] = self.transforms(index, routes, updates)
        del routes
        del updates
        return(None)
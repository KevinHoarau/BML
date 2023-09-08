 #!/usr/bin/env python3

import os, time, multiprocessing
import ujson as json
from BML import utils
from BML.utils import ProcessingQueue
from BML.data.routes import dumpRoutes, getUpdatesInfos, parseUpdate, updateRoutes
from math import ceil

class BaseTransform(utils.BmlProcess):

    computeRoutes = True
    computeUpdates = True
    loadPrimingData = True
    fileExtension = ".json"
    params = {}

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):

        utils.BmlProcess.__init__(self, logFiles)
        
        self.params["Name"] = None
        self.params["Period"] = 5
        self.params["NbSnapshots"] = None
        self.params["Collectors"] = []
        self.params["IpVersion"] = [4,6]
        self.params["SkipIfExist"] = True
        self.params["Gzip"] = False
        
        self.setParams(params)
        
        self.primingFile = primingFile
        self.dataFile = dataFile
        self.outFolder = outFolder
        self.filePath = self.getFilePath(outFolder)
        self.transformedData = []

    def init(self):

        self.log("Init")
        
        if(self.primingFile[-3:]==".gz"):
            utils.printAndLog("Ungzip priming data", self.logFiles)
            utils.ungzipFile(self.primingFile)
            self.primingFile = self.primingFile[:-3]
        
        if(self.dataFile[-3:]==".gz"):
            utils.printAndLog("Ungzip updates", self.logFiles)
            utils.ungzipFile(self.dataFile)
            self.dataFile = self.dataFile[:-3]
        
        self.log("Load priming data")
        self.prior_routes = {}
        if(self.loadPrimingData):
            self.prior_routes = json.load(open(self.primingFile))
            for prefix in list(self.prior_routes.keys()):

                if(utils.ipVersion(prefix) in self.params["IpVersion"]):
        
                    if(len(self.params["Collectors"])>0):

                        for collector in list(self.prior_routes[prefix].keys()):
                            if(not collector in self.params["Collectors"]):
                                del self.prior_routes[prefix][collector]
                else:
                    del self.prior_routes[prefix]
        
        self.log("Get updates infos")
        self.header, self.startTime, self.endTime = getUpdatesInfos(self.dataFile)
        
        if(self.params["NbSnapshots"]==None):
            self.T = ceil((self.endTime-self.startTime)/60 /self.params["Period"]) + 1 # +1 because the first snapshot is at t=0
        else:
            self.T = self.params["NbSnapshots"]
        
        self.startProgress = self.startTime
        self.endProgress= self.endTime

    def computeSnapshot(self, t, routes, updatesParsed):
        self.transformedData.append(self.transforms(t, routes, updatesParsed))

    def compute(self):

        self.log("Compute")

        self.preProcess()

        lines = open(self.dataFile)

        routes = self.prior_routes
        updatesParsed = []

        t = 0
        i=0
        for line in lines:

            if(t == self.T):
                break
                
            if(i!=0):
                
                update = parseUpdate(line[:-1].split(','), self.header)

                if(self.startTime + t*self.params["Period"]*60 <= int(update["time"]) and t < self.T):
                    
                    self.computeSnapshot(t, routes, updatesParsed)
                    updatesParsed = []
                    t += 1

                prefix = update["fields"]["prefix"]

                if(utils.ipVersion(prefix) in self.params["IpVersion"]):
                    if(len(self.params["Collectors"])>0):
                        if(update["collector"] in self.params["Collectors"]):
                            if(self.computeRoutes):
                                routes = updateRoutes(routes, update)
                            if(self.computeUpdates):
                                updatesParsed.append(update)

                    else:
                        if(self.computeRoutes):
                            routes = updateRoutes(routes, update)
                        if(self.computeUpdates):
                            updatesParsed.append(update)

                self.printProgress(int(update["time"]))
            i+=1
                
    def getFileName(self):
            if(self.params["Name"] is None):
                return((self.__class__.__name__ + "_" + str(self.params["Period"])))
            else:
                return(self.params["Name"])
    
    def getFilePath(self, path):
        if(path is None):
            return(None)
        else:
            return(utils.mkdirPath(path) + self.getFileName() + self.fileExtension)

    def save(self):
        with open(self.filePath, 'w') as outfile:
            json.dump(self.transformedData, outfile)
        self.log("saved")

    def exists(self):
        return(os.path.exists(self.filePath) or os.path.exists(self.filePath+".gz"))

    def transforms(self, index, routes, updates):
        return(None)

    def preProcess(self):
        return(None)

    def postProcess(self, transformedData):
        return(transformedData)

    def execute(self):
        
        timeAtStart = time.time()
        
        self.log("###############")
        self.log("# Transform")
        self.log("###############")
        
        self.log("Folder: {}".format(self.outFolder))
        self.printParams()
        
        if(self.filePath!=None and self.exists() and self.params["SkipIfExist"]):
            self.log("Data exists, skipped")
        else:
            self.init()

            self.compute()

            self.transformedData = self.postProcess(self.transformedData)

            self.log("Computation time: " + utils.timeFormat(time.time()-timeAtStart))
            if(self.filePath!=None):
                self.save()
                if(self.params["Gzip"]):
                    utils.gzipFile(self.filePath, remove=True)
                    self.filePath += ".gz"
                self.log("Transformed data saved to: " + self.filePath)
                
            if(os.path.exists(self.dataFile+".gz")):
                os.remove(self.dataFile)
            if(os.path.exists(self.primingFile+".gz")):
                if(self.loadPrimingData):
                    os.remove(self.primingFile)

class BaseTransformParallelized(BaseTransform):

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["nbProcess"] = multiprocessing.cpu_count()
        
        BaseTransform.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
        
    def preProcess(self):
        manager = multiprocessing.Manager()
        self.data = manager.dict()
        self.pq = ProcessingQueue(nbProcess=self.params["nbProcess"])
        return(None)

    def computeSnapshot(self, t, routes, updatesParsed):
        self.pq.waitUntilFree()
        self.data[t] = None
        self.pq.addProcess(target=self.runTransforms, args=(self.data, t, routes, updatesParsed))
        self.pq.runOnce()
        #print("computeSnapshot:",t)

    def compute(self):

        BaseTransform.compute(self)
        
        self.pq.join()

        for i in range(len(self.data)):
            if(i in self.data):
                self.transformedData.append(self.data[i])
                del self.data[i]

    def runTransforms(self, data, index, routes, updates):
        data[index] = self.transforms(index, routes, updates)
        del routes
        del updates
        return(None)
    
def transform(transformation, primingFile, dataFile, params=None, outFolder=None, logFiles=None):
        
    if(logFiles is None):
        logFiles = []
                       
    if(params is None):
        params = {}
    
    if(outFolder!=None):
        logFile = open(utils.mkdirPath(outFolder)+"transform.log",'w')
        logFiles.append(logFile)
    
    transform = transformation(primingFile, dataFile, params, outFolder, logFiles)
    transform.execute()

    if(outFolder!=None):
        logFile.close()

    return(transform.transformedData, transform.filePath)
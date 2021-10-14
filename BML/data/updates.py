#!/usr/bin/env python3

import time, json
from pybgpstream import BGPStream, BGPRecord, BGPElem
from BML import utils

def serialize_sets(obj):
    if isinstance(obj, set):
        return list(obj)
    return obj

class UpdatesDump(utils.BmlProcess):

    def __init__(self, start, end, params, outFolder, logFiles):

        utils.BmlProcess.__init__(self, logFiles)

        self.startTime = start
        self.endTime = end

        self.stream = BGPStream()

        self.filePath = self.getFilePath(outFolder)

        self.params = {
            "Projects": ['ris','routeviews'],
            "Collectors": [],
            "IpVersion": [4,6],
            "UseRibs": False,
        }
        
        self.setParams(params)

    def startStream(self):

        if(self.startTime!=-1 and self.endTime!=-1):

            if(self.params["UseRibs"]):
                self.stream = BGPStream(
                    from_time=self.startTime, until_time=self.endTime,
                    collectors=self.params["Collectors"],
                    projects=self.params["Projects"]
                    )
            else:
                self.stream = BGPStream(
                    from_time=self.startTime, until_time=self.endTime,
                    collectors=self.params["Collectors"],
                    record_type="updates",
                    projects=self.params["Projects"]
                    )
        else:
            quit("Error: can't start stream, interval not set")

    def buildUpdatesDump(self):
        
        self.startProgress = self.startTime
        self.endProgress = self.endTime

        self.emptyFile()

        for record in self.stream.records():

            if record.status == "valid":

                for elem in record:

                    self.printProgress(elem.time)

                    if(elem.type=='A' or elem.type=='W' or elem.type=='R'):
                        
                        if(utils.ipVersion(elem.fields['prefix']) in self.params["IpVersion"]):

                            u = {}

                            u['collector'] = str(record.collector)
                            u['dump_time'] = str(record.dump_time)
                            u['type'] = str(elem.type)
                            u['time'] = str(int(elem.time))
                            u['peer_address'] = str(elem.peer_address)
                            u['peer_asn'] = str(elem.peer_asn)
                            u['fields'] = json.dumps(elem.fields, default=serialize_sets)

                            self.appendToFile(u)

    def getFilePath(self, path):
        return(utils.mkdirPath(path) + "updates.csv")

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

    def execute(self):
        
        timeAtStart = time.time()
        
        self.log("###############")
        self.log("# Updates dump")
        self.log("###############")
        self.log("Start time: " + str(self.startTime))
        self.log("End time: " + str(self.endTime))
        self.log("Duration: " + utils.timeFormat(self.endTime-self.startTime))
        self.printParams()

        self.startStream()
        self.buildUpdatesDump()
        
        self.log("Computation time: " + utils.timeFormat(time.time()-timeAtStart))
        self.log("Updates dump saved to: " + self.filePath)
        

def dumpUpdates(start, end, outfolder, params=None, logFiles=None):
    
    if(params is None):
        params = {}
        
    if(logFiles is None):
        logFiles = []
    
    logFile = open(utils.mkdirPath(outfolder)+"updates_dump.log",'w')
    logFiles.append(logFile)

    updatesDump = UpdatesDump(start, end, params, outfolder, logFiles)
    updatesDump.execute()
    logFile.close()

    return(updatesDump.filePath)
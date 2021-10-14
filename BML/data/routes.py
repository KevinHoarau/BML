#!/usr/bin/env python3

import time, json
from BML import utils

def updateRoutes(routes, u):

    if(u['type']=='A' or u['type']=='W' or u['type']=='R'):

        collector = u['collector']
        peer = u['peer_asn']
        prefix = u['fields']['prefix']

        if(u['type']=='A' or u['type']=='R'):

            if(prefix not in routes):
                routes[prefix] = {}
            if(collector not in routes[prefix]):
                routes[prefix][collector] = {}

            routes[prefix][collector][peer] = u['fields']['as-path']

        elif(u['type']=='W'):

            if(prefix in routes and collector in routes[prefix] and  peer in routes[prefix][collector]):
                del routes[prefix][collector][peer]
                if(len(routes[prefix][collector])==0):
                    del routes[prefix][collector]
                if(len(routes[prefix])==0):
                    del routes[prefix]
                    
    return(routes)

def parseUpdate(lineSplited, header):

    u= {}

    u['collector'] = lineSplited[header['collector']]
    u['dump_time'] = lineSplited[header['dump_time']]
    u['type'] = lineSplited[header['type']]
    u['time'] = lineSplited[header['time']]
    u['peer_address'] = lineSplited[header['peer_address']]
    u['peer_asn'] = lineSplited[header['peer_asn']]
    u['fields'] = json.loads(",".join(lineSplited[header['fields']::]))

    return(u)

def getUpdatesInfos(updates):
    
    
    headerLine = ""
    firstLine = ""
    lastLine = ""
    
    if(type(updates)==str):
        lines = open(updates)
    else:
        lines = updates
    
    i = 0
    for line in lines:
        if(i==0):
            headerLine = line[:-1].split(',')
        if(i==1):
            firstLine = line[:-1].split(',')
        i+= 1
    lastLine = line[:-1].split(',')

    header = utils.getIndexList(headerLine)
    startTime = int(parseUpdate(firstLine, header)["time"])
    endTime = int(parseUpdate(lastLine, header)["time"])
    
    if(type(updates)==str):
        lines.close()
    
    return(header, startTime, endTime)
    
class RoutesSnapshot(utils.BmlProcess):

    def __init__(self, updates, routes, outfolder, logFiles):

        utils.BmlProcess.__init__(self, logFiles)

        self.routes = routes
        self.updates = updates
        
        self.filePath = self.getFilePath(outfolder)
    
    def buildRoutesSnapshot(self):

        header, self.startProgress, self.endProgress = getUpdatesInfos(self.updates)
        
        if(type(self.updates)==str):
            lines = open(self.updates)
        else:
            lines = self.updates

        i=0
        for line in lines:
            
            if(i!=0):
                u= parseUpdate(line[:-1].split(','), header)
                self.printProgress(int(u['time']))
                self.routes = updateRoutes(self.routes, u)
            i+=1
        
        if(type(self.updates)==str):
            lines.close()
            
    def getFilePath(self, path):
        if(path is None):
            return(None)
        else:
            return(utils.mkdirPath(path) + "routes.json")

    def save(self):
        file = open(self.filePath,"w")
        json.dump(self.routes, file)
        file.close()

    def execute(self):
        
        timeAtStart = time.time() 
        
        self.log("#################")
        self.log("# Route Snapshot")
        self.log("#################")

        if(type(self.updates)==str):
            self.log("Updates file: " + self.updates)
        else:
            self.log("Nb. of updates: " + str(len(self.updates)))

        self.log("Nb. of prefix in initial route snapshot: " + str(len(self.routes)))

        self.buildRoutesSnapshot()
            
        self.log("Computation time: " + utils.timeFormat(time.time()-timeAtStart))
        self.log("Nb. of prefix in route snapshot: " + str(len(self.routes.keys())))

        if(self.filePath!=None):
            self.save()
            self.log("Route snapshot saved to: " + self.filePath)

def dumpRoutes(updates, routes=None, outFolder=None, logFiles=None):
    
    if(routes is None):
        routes = {}
        
    if(logFiles is None):
        logFiles = []
    
    if(outFolder!=None):
        logFile = open(utils.mkdirPath(outFolder)+"routes_snapshot.log",'w')
        logFiles.append(logFile)
    
    routesSnapshot = RoutesSnapshot(updates, routes, outFolder, logFiles)
    routesSnapshot.execute()

    if(outFolder!=None):
        logFile.close()

    return(routesSnapshot.routes, routesSnapshot.filePath)
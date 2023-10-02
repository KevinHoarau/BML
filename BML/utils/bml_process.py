import sys
from BML import utils

class BmlProcess():
    
    def __init__(self, logFiles):
        
        self.startProgress = 0
        self.endProgress = 0
        self.progressPrev = 0
        
        self.logFiles = logFiles
        
    def setParams(self, params):
        for k,v in params.items():
            if(k in self.params):
                self.params[k] = v
            else:
                sys.exit("Unrecognized parameter:"+k)
                
    def printParams(self):
        self.log("Params:")
        for k,v in self.params.items():
            self.log("  "+k+": " + str(v))
                
    def log(self, text):
        utils.printAndLog(text, self.logFiles)
                
    def printProgress(self, currentProgress):
        if(self.startProgress<self.endProgress):
            progress = int(currentProgress -self.startProgress)*100//(self.endProgress-self.startProgress)
        else:
            progress = 0
        if((progress<=10 and progress!=self.progressPrev) or progress-self.progressPrev>=5):
            self.log("Progress: "+ str(progress) + "%")
            self.progressPrev = progress
"""Summary
"""

import sys, os, time, gzip, shutil, io, multiprocessing, json
from datetime import datetime

def getIndexList(l):
    index = {}
    for i in range(len(l)):
        index[l[i]] = i

    return(index)

def timeFormat(time):
    """Summary
    
    Args:
        time (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    time = int(time)
    seconds = time%60
    time = time//60
    minutes = (time)%60
    time = time//60
    hours = time%60

    timeStr = str(hours) +"h " + str(minutes) + "m " + str(seconds) + "s"
    
    return(timeStr)

def mkdirPath(path):
    """
    Create all folder in a path
    
    Args:
        path (str): a path
    
    Returns:
        str: the created path
    """
    folderPath = ""
    for folder in path.split(os.sep):
        if(folder!=""):
            folderPath += folder + os.sep
            if(not os.path.isdir(folderPath)):
                os.mkdir(folderPath)

    return(folderPath)

def printAndLog(line, files, indent="  "):
    """Summary
    
    Args:
        line (TYPE): Description
        files (TYPE): Description
        indent (str, optional): Description
    """
    nbFiles = len(files)-1
    if(not files[0]=="LOG_ONLY"):
        print(indent*nbFiles + line)
    count = 0
    for file in files:
        if(not file=="LOG_ONLY" ):
            file.write(indent*(nbFiles-count) + line + os.linesep)
            file.flush()
            count += 1

def printProgress(pObject, logFiles):
    """Summary
    
    Args:
        pObject (TYPE): Description
        logFiles (TYPE): Description
    """
    progressPrev = 0
    while pObject.isRunning():
        progress = pObject.getProgress()
        if((progress<=10 and progress!=progressPrev) or progress-progressPrev>=5):
            printAndLog("Progress: "+ str(progress) + "%", logFiles)
            progressPrev = progress
        time.sleep(1)

def getTimestamp(y,m,d,h,mn,s):
    """Summary
    
    Args:
        y (TYPE): Description
        m (TYPE): Description
        d (TYPE): Description
        h (TYPE): Description
        s (TYPE): Description
    
    Returns:
        TYPE: Description
    """
    return(int((datetime(y,m,d,h,mn,s) - datetime(1970, 1, 1)).total_seconds()))


def gzipFile(filepath, remove=True):
    """Summary
    
    Args:
        filepath (TYPE): Description
        remove (bool, optional): Description
    
    Returns:
        TYPE: Description
    """
    filegzpath = filepath + ".gz"
    with open(filepath, 'rb') as f_in:
        with gzip.open(filegzpath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if(remove):
        os.remove(filepath)
    return(filegzpath)

def ungzipFile(filegzpath, remove=False):
    """Summary
    
    Args:
        filegzpath (TYPE): Description
        remove (bool, optional): Description
    
    Returns:
        TYPE: Description
    """
    filepath = filegzpath.split(".gz")[0]
    with gzip.open(filegzpath, 'rb') as f_in:
        with open(filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if(remove):
        os.remove(filegzpath)
    return(filepath)

def ungzipFileInMemory(filegzpath):

    f_out = io.BytesIO()

    with gzip.open(filegzpath, 'rb') as f_in:

        shutil.copyfileobj(f_in, f_out)
        f_in.close()
        f_out.seek(0)

    return(f_out)


class ProcessingQueue(object):

    """Summary
    
    Attributes:
        finish (list): Description
        nbProcess (TYPE): Description
        processes (list): Description
        queue (list): Description
        running (list): Description
    """
    
    def __init__(self, nbProcess=16):
        """Summary
        
        Args:
            nbProcess (int, optional): Description
        """
        self.queue = []
        self.running = []
        self.finish = []
        self.processes = []
        self.nbProcess = nbProcess

        for i in range(self.nbProcess):
            self.processes.append(None)
            self.running.append(None)

    def stop(self):
        for i in range(self.nbProcess):
            self.processes[i].terminate()

    def runOnce(self):

        processesAlive = False
        for i in range(self.nbProcess):
            if(not self.processes[i] is None and self.processes[i].is_alive()):
                processesAlive = True
            else:
                if(self.running[i]!=None):
                    self.finish.append(self.running[i])
                    self.running[i] = None
                    if(self.processes[i].exitcode!=0):
                        sys.exit("Subprocess terminated with exit code %i, execution stoped" % (self.processes[i].exitcode))

                if(len(self.queue)>0):
                    (target, args, kwargs) = self.queue[0]
                    self.processes[i] = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
                    self.processes[i].start()
                    processesAlive = True
                    self.running[i] = (target, args, kwargs)
                    self.queue.pop(0)
        return(processesAlive)

    def waitUntilFree(self):
        while True:
            for i in range(self.nbProcess):
                if(self.processes[i] is None or not self.processes[i].is_alive()):
                    return
            time.sleep(1)


    def run(self, logFilePath=""):
        """Summary
        
        Args:
            logFilePath (str, optional): Description
        """
        processesAlive = False

        try:
            
            while len(self.queue)>0 or processesAlive:

                processesAlive = self.runOnce()

                self.runLog(logFilePath)
                time.sleep(1)

            for i in range(self.nbProcess):
                if(not self.processes[i] is None):
                    self.processes[i].join()
                    self.processes[i].close()

        except KeyboardInterrupt:
            print("ProcessingQueue: terminate running jobs")

            for i in range(self.nbProcess):
                if(not self.processes[i] is None):
                    self.processes[i].terminate()
                    self.processes[i].join()
                    self.processes[i].close()

            print("ProcessingQueue: done")

    def addProcess(self, target=None, args=(), kwargs={}):
        """Summary
        
        Args:
            target (None, optional): Description
            args (tuple, optional): Description
            kwargs (dict, optional): Description
        """
        self.queue.append((target, args, kwargs))

    def formatLog(self, listP):
        """Summary
        
        Args:
            listP (TYPE): Description
        
        Returns:
            TYPE: Description
        """
        i_space = len(str(len(listP)))
        t_space = len("Function")
        a_space = len("Args")
        kw_space = len("Kwargs")
        log = ""

        for i in range(len(listP)):
            if(listP[i]!=None):
                (target, args, kwargs) = listP[i]
                t_space = len(str(target.__name__)) if len(str(target.__name__))>t_space else t_space
                a_space = len(str(args)) if len(str(args))>a_space else a_space
                kw_space = len(str(kwargs)) if len(str(kwargs))>kw_space else kw_space

        vline = ("="*(t_space+a_space+kw_space+13)) + "\n"
        log+= vline
        log += ("|{:<"+str(i_space)+"s}| {:"+str(t_space)+"s} | {:"+str(a_space)+"s} | {:"+str(kw_space)+"s} | \n").format("#","Function","Args","Kwargs")
        descr = "|{:<"+str(i_space)+"d}| {:"+str(t_space)+"s} | {:"+str(a_space)+"s} | {:"+str(kw_space)+"s} | \n"
        log+= vline

        for i in range(len(listP)):
            if(listP[i]!=None):
                (target, args, kwargs) = listP[i]
                log += descr.format(i, target.__name__, str(args), str(kwargs))
            else:
                log += descr.format(i, "Empty", "", "")

        log += vline

        return(log)

    def runLog(self, logFilePath):
        """Summary
        
        Args:
            logFilePath (TYPE): Description
        """
        if(not logFilePath==""):

            log =  "#######################\n"
            log += "# Queue : Running      \n"
            log += "#######################\n"
            log += self.formatLog(self.running) + "\n"

            log +=  "#######################\n"
            log += "# Queue : Waiting      \n"
            log += "#######################\n"
            log += self.formatLog(self.queue) + "\n"

            log +=  "#######################\n"
            log += "# Queue : Finish      \n"
            log += "#######################\n"
            log += self.formatLog(self.finish) + "\n"

            with open(logFilePath, "w") as file:
                file.write(log)
                file.close()



class MissingArg(Exception):
    """
    Exeception class for missing args
    """
    pass

def printHelp(help):
    """
    Print help for usage of a command
    Args:
        help (str): help text
    """
    print("")
    print(" Usage :")
    print("   "+sys.argv[0]+help)
    print("")

def getArg(arg, optional=False):
    """
    Get an argument from the command line, (e.g. "-a" for the value after "-a" in the command line).
    If the the arg is missing, return None if the is arg is optional otherwise a MissingArg exception is raised.
    
    Args:
        arg (str): arg to get 
        optional (bool): argument to get
    
    Returns:
        str: arg value
    """
    if(arg in sys.argv):
        if(len(sys.argv) > (sys.argv.index(arg)+1)):
            return(sys.argv[sys.argv.index(arg)+1])
    if(optional):
        return(None)
    raise(MissingArg("Missing argument : "+arg))

def getIntArg(arg, optional=False):
    """
    Similar to "getArg" but return the integer value of the arg.
    
    Args:
        arg (str): arg to get 
        optional (bool): argument to get
    
    Returns:
        int: arg value
    """
    return(int(getArg(arg, optional)))



def saveJobs(jobs, folder, parts=1,  indent=None):
    """
    Dump a jobs list to multiple parts.
    
    Args:
        jobs (list): a jobs list
        folder (str): the output folder
        parts (int, optional): the number of parts
    """
    nbElem = (len(jobs)//parts) + 1

    for i in range(parts):

        start = i*nbElem
        end = (i+1)*nbElem
        
        if(end>len(jobs)):
            end = len(jobs)
            
        filepath = "{}{}.json".format(mkdirPath(folder), i)

        with open(filepath, "w") as file:
            json.dump(jobs[start:end], file, indent=indent)

        file.close()


def runJobs(jobs, folder, nbProcess=1):

    processingQueue = ProcessingQueue(nbProcess=nbProcess)

    logFiles = []
    folder = mkdirPath(folder)
    logFile = open(folder+"jobs.log",'w')
    logFiles.append(logFile)

    printAndLog("################", logFiles)
    printAndLog("# Run jobs list ", logFiles)
    printAndLog("################", logFiles)

    timeAtStart = time.time()

    printAndLog("Number of processes to execute: {}".format(len(jobs)), logFiles)
    printAndLog("Number of processes in parallel: {}".format(nbProcess), logFiles)

    
    for j in jobs:

        exec(j["includes"])
        processingQueue.addProcess(target=locals()[j["target"]], args=j["args"], kwargs=j["kwargs"])

    
    printAndLog("Processing queue: started", logFiles)
    printAndLog("To monitor the execution run: watch -n 1 cat {}queue.log".format(folder), logFiles)
    processingQueue.run(logFilePath= "{}queue.log".format(folder))

    printAndLog("Processing queue: finish", logFiles)
    printAndLog("Computation time: {}".format(timeFormat(time.time()-timeAtStart)), logFiles)

    logFile.close()
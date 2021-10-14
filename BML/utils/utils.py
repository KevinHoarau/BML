"""Summary
"""

import sys, os, time, gzip, shutil, io, json
from datetime import datetime
from .processing_queue import ProcessingQueue

def ipVersion(prefix):
    if(":"in prefix):
        return(6)
    else:
        return(4)

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
    if(len(files)==0 or not files[0]=="LOG_ONLY"):
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
    while pObject.is_alive():
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
    #processingQueue.run(logFilePath= "{}queue.log".format(folder))

    from tqdm.auto import tqdm

    bar = tqdm(total=len(jobs))
    prev = 0
    while(len(processingQueue.finish)<len(jobs)):
        processingQueue.runOnce()
        val = len(processingQueue.finish)
        if(val!=prev):
            bar.update(val-prev)
            prev=val

        processingQueue.runLog("{}queue.log".format(folder))
        processingQueue.waitUntilFree()
            
    processingQueue.run()

    printAndLog("Processing queue: finish", logFiles)
    printAndLog("Computation time: {}".format(timeFormat(time.time()-timeAtStart)), logFiles)

    logFile.close()


def getTransform(folder, transform, name=None, period=2, gzip=False):
    
    data = {}
    
    excludedFolders = ['transform_jobs', 'collect_jobs']
    
    filename = ""
    if(name is None):
        filename += transform + "_" + str(period)
    else:
        filename += name
        
    filename += ".json"
    
    if(gzip):
         filename += ".gz"
    
    for label in os.listdir(folder):
        if(label not in excludedFolders):
            data[label] = {}
            
            for sample in os.listdir(folder+"/"+label):

                if(sample not in ['.ipynb_checkpoints']):

                
                    filepath = folder+"/"+label + "/" + sample +  "/transform/" + transform + "/" + filename
                    
                    if(os.path.exists(filepath)):
                        data[label][sample] = json.load(open(filepath))
                
    return(data)

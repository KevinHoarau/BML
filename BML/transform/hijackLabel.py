from BML.transform import BaseTransformParallelized

class HijackLabel(BaseTransformParallelized):
    
    computeUpdates = False
        
    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["hijack"] = None
        
        BaseTransformParallelized.__init__(self, primingFile, dataFile, params, outFolder, logFiles)

    def transforms(self, index, routes, updates):
        
        hj_origin = 0
        hj_path = 0

        if(not self.params["hijack"] is None):

            hj_prefix = self.params["hijack"]["prefix"]
            hj_as = self.params["hijack"]["hijack_as"]

            if(hj_prefix in routes):
                for collector in routes[hj_prefix].keys():
                    for peer in routes[hj_prefix][collector].keys():

                        if(not routes[hj_prefix][collector][peer] is None):

                            path = routes[hj_prefix][collector][peer].split(" ")

                            if(str(path[-1])==str(hj_as)):
                                hj_origin = 1

                            if(str(hj_as) in path[:-1]):
                                hj_path = 1
                                
        label = {"origin":hj_origin, "path":hj_path}

        return(label)

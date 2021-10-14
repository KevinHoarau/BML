from BML.transform import BaseTransformParallelized
import ipaddress as ip

class HijackAnalyze(BaseTransformParallelized):

    computeRoutes = False
    loadPrimingData = False
    
    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["hijack"] = None
        self.params["summary"] = True
        
        BaseTransformParallelized.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
        
    def transforms(self, index, routes, updates):
        
        prefix_origin = set()
        prefix_path = set()
        subprefix_origin = set()
        subprefix_path = set()

        if(not self.params["hijack"] is None):
            
            prefixes = self.params["hijack"]["prefixes"]
            hj_as = str(self.params["hijack"]["hijack_as"])
            
            for update in updates:
                
                if(update["type"]=='A'):

                    prefix = update["fields"]["prefix"]
                    path = update["fields"]['as-path'].split(" ")
                    
                    #prefix
                    if(prefix in prefixes):

                        # origin hijack
                        if(str(path[-1])==hj_as):
                            prefix_origin.add(prefix)

                        # path hijack
                        elif(hj_as in path):
                            prefix_path.add(prefix)
                    
                    #subprefix
                    for p in prefixes:
                        prefix_v = ip.ip_network(p)
                        prefix_h = ip.ip_network(prefix)        
            
                        if(prefix_v != prefix_h and prefix_v.version==prefix_h.version and prefix_h.subnet_of(prefix_v) ):

                            # origin hijack
                            if(str(path[-1])==hj_as):
                                subprefix_origin.add(prefix)

                            # path hijack
                            elif(hj_as in path):
                                subprefix_path.add(prefix)
                    
        r = {
            "prefix_origin": list(prefix_origin),
            "prefix_path": list(prefix_path),
            "subprefix_origin": list(subprefix_origin),
            "subprefix_path": list(subprefix_path)
        }
        
        return(r)

    def transformsOld(self, index, routes, updates):
        
        prefix_origin = set()
        prefix_path = set()
        subprefix_origin = set()
        subprefix_path = set()

        if(not self.params["hijack"] is None):

            prefixes = self.params["hijack"]["prefixes"]
            hj_as = str(self.params["hijack"]["hijack_as"])
            
            for prefix in prefixes:
                
                #prefix
                if(prefix in routes):
                    for collector in routes[prefix].keys():
                        for peer in routes[prefix][collector].keys():

                            if(not routes[prefix][collector][peer] is None):

                                path = routes[prefix][collector][peer].split(" ")
                                
                                # origin hijack
                                if(str(path[-1])==hj_as):
                                    prefix_origin.add(prefix)
                                
                                # path hijack
                                elif(hj_as in path):
                                    prefix_path.add(prefix)
                """           
                #subprefix
                for p in routes.keys():
                    
                    prefix_v = ip.ip_network(prefix)
                    prefix_h = ip.ip_network(p)
                    
                    if(prefix_v != prefix_h and prefix_v.version==prefix_h.version and prefix_h.subnet_of(prefix_v) ):
                
                        for collector in routes[p].keys():
                            for peer in routes[p][collector].keys():

                                if(not routes[p][collector][peer] is None):

                                    path = routes[p][collector][peer].split(" ")

                                    # origin hijack
                                    if(str(path[-1])==hj_as):
                                        subprefix_origin.add(prefix)

                                    # path hijack
                                    elif(hj_as in path):
                                        subprefix_path.add(prefix)
                """
                                    
        r = {
            "prefix_origin": list(prefix_origin),
            "prefix_path": list(prefix_path),
            "subprefix_origin": list(subprefix_origin),
            "subprefix_path": list(subprefix_path)
        }
        
        return(r)
    
    def postProcess(self, transformedData):

        if(self.params["summary"]==True):

            prefix_origin = set()
            prefix_path = set()
            subprefix_origin = set()
            subprefix_path = set()

            for d in transformedData:
                for v in d["prefix_origin"]:
                    prefix_origin.add(v)

                for v in d["prefix_path"]:
                    prefix_path.add(v)

                for v in d["subprefix_origin"]:
                    subprefix_origin.add(v)

                for v in d["subprefix_path"]:
                    subprefix_path.add(v)

            r = {
            "prefix_origin": list(prefix_origin),
            "prefix_path": list(prefix_path),
            "subprefix_origin": list(subprefix_origin),
            "subprefix_path": list(subprefix_path)
            }

            return(r)

        return(transformedData)

from BML.transform import BaseTransform
import editdistance

def buildRoutesAS(routes):
    routesAS = {}

    for prefix in routes.keys():
        for collector in routes[prefix].keys():
            for peer in routes[prefix][collector].keys():
                path = routes[prefix][collector][peer]

                if(path != None and not '{' in path and not '}' in path):
                    
                    path_vertices = path.split(' ')
                        
                    for i in range(len(path_vertices)):                       
                        asn = path_vertices[i]
                        
                        if(not asn in routesAS):
                            routesAS[asn] = {}
                            
                        routesAS[asn][prefix] = path_vertices[i:]
    return(routesAS)
                        

class ASFeatures(BaseTransform):
    
    computeUpdates = False
    oldRoutesAS = None

    def transforms(self, index, routes, updates):
        
        features = {}
        routesAS = buildRoutesAS(routes)
        
        if(index>0):
            
            asns = set(list(routesAS.keys()) + list(self.oldRoutesAS.keys()))
            for asn in asns:
                features[asn] = {
                    "nb_routes":0,
                    "nb_new_routes":0,
                    "nb_withdrawals":0,
                    "nb_origin_change":0,
                    "nb_route_change":0,
                    "max_path_len" : 0,
                    "avg_path_len" : 0,
                    "max_editdist" : 0,
                    "avg_editdist" : 0,
                    "nb_tolonger" : 0, 
                    "nb_toshorter" : 0,
                }

            for asn in routesAS.keys():
                
                nbRoutes = len(routesAS[asn])
                sum_path_len = 0
                sum_editdist = 0
                
                for prefix in routesAS[asn].keys():
                    
                    if(asn in self.oldRoutesAS and prefix in self.oldRoutesAS[asn]):
                        
                        path =  routesAS[asn][prefix]
                        pathOld = self.oldRoutesAS[asn][prefix]
                        
                        if(path!=pathOld):
                            features[asn]["nb_route_change"] += 1
                            
                        if(path[-1]!=pathOld[-1]):
                            features[asn]["nb_origin_change"] += 1
                            
                        path_length = len(path)
                        pathOld_length = len(pathOld)
                        
                        if(path_length>pathOld_length):
                            features[asn]["nb_tolonger"] += 1
                        if(path_length<pathOld_length):
                            features[asn]["nb_toshorter"] += 1
                            
                        sum_path_len += path_length
                        if(path_length > features[asn]["max_path_len"]):
                            features[asn]["max_path_len"] = path_length
                            
                        edist = editdistance.eval(path,pathOld)
                        
                        sum_editdist += edist
                        if(edist > features[asn]["max_editdist"]):
                            features[asn]["max_editdist"] = edist
                            
                    else:
                        features[asn]["nb_new_routes"] += 1
                 
                    features[asn]["nb_routes"] = nbRoutes
                    features[asn]["avg_path_len"] = sum_path_len/nbRoutes
                    features[asn]["avg_editdist"] = sum_editdist/nbRoutes
                
            for asn in self.oldRoutesAS.keys():
                
                for prefix in self.oldRoutesAS[asn].keys():
        
                    if(not asn in routesAS or not prefix in routesAS[asn]):
                        features[asn]["nb_withdrawals"] += 1
                
        
        self.oldRoutesAS = routesAS

        return(features)

    def postProcess(self, transformedData):
        transformedData.pop(0)
        return(transformedData)
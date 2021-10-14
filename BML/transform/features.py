import editdistance
from BML.transform import BaseTransform

class Features(BaseTransform):

    computeRoutes = False

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        BaseTransform.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
        
    def transforms(self, index, routes, updates):

        features = {
            "nb_A" : 0, # Number of announcements
            "nb_W" : 0, # Number of withdrawals:
            "nb_implicit_W" : 0,  
            "nb_dup_A" : 0,
            "nb_dup_W" : 0,
            "nb_A_prefix" : 0, # Number of announced prefixes
            "nb_W_prefix" : 0, # Number of withdrawn prefixes
            "max_A_prefix" : 0, # Max. announcements per prefix
            "avg_A_prefix" : 0, # Avg. announcements per prefix
            "max_A_AS" : 0, # Max. announcements per AS
            "avg_A_AS" : 0, # Avg. announcements per AS
            "nb_orign_change" : 0, 
            "nb_new_A" : 0, # Not stored in RIB
            "nb_new_A_afterW" : 0, 
            "max_path_len" : 0,
            "avg_path_len" : 0,
            "max_editdist" : 0,
            "avg_editdist" : 0,
            "editdist_7" : 0,
            "editdist_8" : 0,
            "editdist_9" : 0,
            "editdist_10" : 0,
            "editdist_11" : 0,
            "editdist_12" : 0,
            "editdist_13" : 0,
            "editdist_14" : 0,
            "editdist_15" : 0,
            "editdist_16" : 0,
            "editdist_17" : 0,
            "nb_tolonger" : 0, 
            "nb_toshorter" : 0,
            "avg_interarrival" : 0,
        }

        if(len(updates)<2):
            return(features)

        c = 0

        prevTime = 0

        A_per_prefix = {}
        W_per_prefix = {}

        A_per_AS = {}

        path_len = []
        inter_time = []
        editDist = []

        for update in updates:    

            if(update["fields"]["prefix"] not in routes):
                    routes[update["fields"]["prefix"]] = {}
            if(update["collector"] not in routes[update["fields"]["prefix"]]):
                routes[update["fields"]["prefix"]][update["collector"]] = {}

            if(update["type"]=='A'):

                features["nb_A"] += 1

                Uas_path = update["fields"]['as-path']

                path_len.append(len(Uas_path.split(" ")))

                if(update["fields"]["prefix"] not in A_per_prefix):
                    A_per_prefix[update["fields"]["prefix"]] = 0

                A_per_prefix[update["fields"]["prefix"]] += 1

                origin = Uas_path.split(" ")[-1]
                if(origin not in A_per_AS):
                    A_per_AS[origin] = 0

                A_per_AS[origin] += 1


                if(update["peer_asn"] not in routes[update["fields"]["prefix"]][update["collector"]] or routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]]==None):
                    features["nb_new_A"] += 1
                elif(routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]]=="w"+str(index)):
                    features["nb_new_A_afterW"] += 1
                elif(routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]][0]=="w"):
                    features["nb_new_A"] += 1
                elif(routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]]==Uas_path):
                    features["nb_dup_A"] += 1
                else:
                    features["nb_implicit_W"] += 1

                    ASlist_prev = routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]].split(" ")
                    ASlist_new = Uas_path.split(" ")

                    if(ASlist_prev[-1] != ASlist_new[-1]):
                        features["nb_orign_change"] += 1

                    if(len(ASlist_new) > len(ASlist_prev)):
                        features["nb_tolonger"] += 1
                    else:
                        features["nb_toshorter"] += 1

                    edist = editdistance.eval(ASlist_prev,ASlist_new)
                    editDist.append(edist)
                    if(edist >= 7 and edist <= 17):
                        features["editdist_"+str(edist)] += 1

                routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]] = Uas_path

            elif(update["type"]=='W'):

                features["nb_W"] += 1

                if(update["fields"]["prefix"] not in W_per_prefix):
                    W_per_prefix[update["fields"]["prefix"]] = 0

                W_per_prefix[update["fields"]["prefix"]] += 1

                if(update["peer_asn"] in routes[update["fields"]["prefix"]][update["collector"]] and routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]]=="w"+str(index)):

                    features["nb_dup_W"] += 1

                routes[update["fields"]["prefix"]][update["collector"]][update["peer_asn"]] = "w"+str(index)

            if(prevTime!=0):
                iTime = int(update["time"]) - prevTime
                if(iTime > 0):
                    inter_time.append(iTime)

            prevTime = int(update["time"])

        features["nb_A_prefix"] = len(A_per_prefix)
        features["nb_W_prefix"] = len(W_per_prefix)
        A_per_prefix_values = A_per_prefix.values()
        if(len(A_per_prefix_values)>1):
            features["max_A_prefix"] = max(A_per_prefix_values)
            features["avg_A_prefix"] = round(sum(A_per_prefix_values) / len(A_per_prefix_values))

        A_per_AS_values = A_per_AS.values()
        if(len(A_per_AS_values)>1):
            features["max_A_AS"] = max(A_per_AS_values)
            features["avg_A_AS"] = round(sum(A_per_AS_values) / len(A_per_AS_values))

        if(len(path_len)>1):
            features["max_path_len"] =  max(path_len)
            features["avg_path_len"] = round(sum(path_len) / len(path_len))

        if(len(inter_time)>1):
            features["avg_interarrival"] = round(sum(inter_time) *1000/ len(inter_time))

        if(len(editDist)>1):
            features["max_editdist"] =  max(editDist)
            features["avg_editdist"] = round(sum(editDist) / len(editDist))

        return(features)

    def postProcess(self, transformedData):
        transformedData.pop(0)
        return(transformedData)


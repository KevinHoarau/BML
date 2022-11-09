from .graph import Graph
import networkx as nx
import torch

class TemporalGraph(Graph):

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        params["relabel_nodes"] = False
        params["weighted"] = True
        
        Graph.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
    
    def postProcess(self, G):

        nodes = set()
        for g in G:
            for n in list(g.nodes):
                nodes.add(n)
        nodes = list(nodes)

        mapping = {}
        i = 0
        for n in nodes:
            mapping[n]=i
            i += 1

        data = {
            "ASN":nodes,
            "edge_index":[],
            "edge_attr":[],
            "x":[]
        }

        for g in G:

            g_new = nx.relabel_nodes(g, mapping, copy=True)
            edges = list(g_new.edges)

            edge_index = torch.LongTensor(edges)
            edge_attr = torch.tensor([[g_new[u][v]["nbIp"]] for u,v in edges])

            x = []
            for i in range(len(nodes)):
                if(i in g_new.nodes):
                    x.append([g_new.nodes[i]["nbIp"]])
                else:
                    x.append([0])
            x = torch.tensor(x)

            data["edge_index"].append(edge_index)
            data["edge_attr"].append(edge_attr)
            data["x"].append(x)

        return(data)

        
    
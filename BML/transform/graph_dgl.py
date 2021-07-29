from BML.transform.graph import Graph
import networkx as nx
import dgl

def nx2dgl(G):
    G = nx.convert_node_labels_to_integers(G)

    src_ids = []
    dst_ids = []
    for l in nx.generate_edgelist(G):
        s,d,_ = l.split(" ")
        src_ids.append(int(s))
        dst_ids.append(int(d))

    return(dgl.graph((src_ids, dst_ids)))

class GraphDGL(Graph):
    
    def __init__(self):
        Graph.__init__(self)
    
    def transforms(self, index, G):
    	return(nx2dgl(G))

    def saveData(self, folder, fileName, data, gzip):

        filePath = folder + fileName + ".dgl"

        with open(filePath, 'w') as outfile:
            dgl.save_graphs(filePath, data)

        if(gzip):
            utils.gzipFile(filePath)
            filePath += ".gz"
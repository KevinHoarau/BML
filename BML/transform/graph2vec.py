from BML.transform import Graph, BaseTransform
from karateclub import Graph2Vec as G2V
import networkx as nx

class Graph2Vec(Graph):
    
    fileExtension = ".json"
    
    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["dimension"] = 16

        Graph.__init__(self, primingFile, dataFile, params, outFolder, logFiles)

    def save(self):
        BaseTransform.save(self)
        
    def postProcess(self, transformedData):
        
        for i in range(len(transformedData)):
            transformedData[i] = nx.convert_node_labels_to_integers(transformedData[i])
        
        model = G2V(dimensions=self.params["dimension"],workers=self.params["nbProcess"])
        model.fit(transformedData)
        transformedData = model.get_embedding().tolist()
        
        return(transformedData)
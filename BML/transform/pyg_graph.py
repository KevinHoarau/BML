from .graph import Graph
import torch
from torch_geometric.utils import from_networkx

class PyGgraph(Graph):
    
    fileExtension = ".pt"

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        Graph.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
    
    def save(self):
        torch.save(self.transformedData, self.filePath)
        self.log("saved")

    def transforms(self, index, G):
        pyG = from_networkx(G)
        return(pyG)
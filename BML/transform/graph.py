from BML.transform import BaseTransform, BaseTransformParallelized
from BML.utils import ProcessingQueue

import networkit as nk
import networkx as nx

import numpy as np
import pandas as pd

import multiprocessing, pickle, time

def buildWeightedGraph(routes):

    graph =  nx.Graph()

    for prefix in routes.keys():

        #nbIp = 2 ** (32-int(prefix.split('/')[1]))
        nbIp = 1

        origins = []
        vertices = []
        edges = []

        for collector in routes[prefix].keys():
            
            for peer in routes[prefix][collector].keys():
                path = routes[prefix][collector][peer]
                path_vertices = []
                path_edges = []
                path_origin = ""

                if(path != None):

                    if('{' in path or '}' in path):
                        pass
                    else:
                        path_vertices = path.split(' ')
                        path_origin = path_vertices[-1]
                        for i in range(len(path_vertices)-1):
                            path_edges.append([path_vertices[i], path_vertices[i+1]])

                        if(path_origin not in origins):
                            origins.append(path_origin)

                        for vertex in path_vertices:
                            if(vertex not in vertices):
                                vertices.append(vertex)

                        for edge in path_edges:
                            if(edge not in edges):
                                edges.append(edge)

        for vertex in vertices:
            if(not graph.has_node(vertex)):
                graph.add_node(vertex, nbIp=0)

            
        for (a,b) in edges:
            if(not graph.has_edge(a,b)):
                graph.add_edge(a,b, nbIp=0)

            graph[a][b]['nbIp'] += nbIp

        for origin in origins:
            graph.nodes[origin]['nbIp'] += nbIp

    return(graph)

def buildGraph(routes):

    G =  nx.Graph()

    edges = set()

    for prefix in routes.keys():

        for collector in routes[prefix].keys():
            
            for peer in routes[prefix][collector].keys():
                path = routes[prefix][collector][peer]

                if(path != None):

                    if('{' in path or '}' in path):
                        pass
                    else:

                        path_vertices = path.split(' ')

                        for i in range(len(path_vertices)-1):
                            a,b = (path_vertices[i], path_vertices[i+1])
                            if(a!=b):
                                edges.add((a,b))
    
    G.add_edges_from(edges)

    return(G)

class Graph(BaseTransformParallelized):
    
    computeUpdates = False
    
    fileExtension = ".pickle"

    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["relabel_nodes"] = False
        self.params["weighted"] = False

        BaseTransformParallelized.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
        self.prevT = 0

    def computeSnapshot(self, t, routes, updatesParsed):
        
        self.pq.waitUntilFree()
        self.pq.addProcess(target=self.runBuildGraph, args=(self.data, t, routes))
        self.pq.runOnce()
        
        if(t!=0 and t%self.params["nbProcess"]==0 or t+1==self.T):
            
            self.pq.join()
            
            for i in range(self.prevT,t+1):
                self.pq.waitUntilFree()
                self.pq.addProcess(target=self.runTransforms, args=(self.data, i, self.data[i]))
                self.pq.runOnce()
            
            self.prevT = t+1
            
    def runBuildGraph(self, data, index, routes):
        if(self.params["weighted"]):
            G = buildWeightedGraph(routes)
        else:
            G = buildGraph(routes)
            
        if(self.params["relabel_nodes"]):
            G = nx.convert_node_labels_to_integers(G, label_attribute="ASN")
        data[index] = G
        del routes
        return(None)
    
    def runTransforms(self, data, index, G):
        data[index] = self.transforms(index, G)
        del G
        return(None)
    
    def save(self):
        with open(self.filePath, 'wb') as outfile:
            pickle.dump(self.transformedData, outfile)
        self.log("saved")

    def transforms(self, index, G):
        return(G)

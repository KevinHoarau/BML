from BML.transform import BaseTransform, BaseTransformParallelized

import networkit as nk
import networkx as nx
import ipaddress as ip

import numpy as np

import multiprocessing 


def buildWeightedGraph(routes):

    graph =  nx.Graph()

    for prefix in routes.keys():

        network = ip.ip_network(prefix)
        nbIp = network.num_addresses

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


def getEdges(routes, keys, edgeslist, index):

    try:

        edges = set()

        for prefix in keys:

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
        edgeslist[index] = edges

    except KeyboardInterrupt:
        pass


def buildGraphParallelized(routes, nbProcess=16):

    G =  nx.Graph()

    manager = multiprocessing.Manager()
    edgeslist = manager.dict()

    keys = [*routes.keys()]
    jobs = []
    try:
        for i in range(nbProcess):
            p = multiprocessing.Process(target=getEdges, args=(routes, keys[i::nbProcess], edgeslist, i))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
            p.close()

    except KeyboardInterrupt:
        print("buildGraphParallelized: terminate running jobs")
        for p in jobs:
            p.terminate()
            p.join()
            p.close()
        print("buildGraphParallelized: done")
    
    l = []
    for i in range(len(edgeslist)):
        for e in edgeslist[i]:
            l.append(e)
        del edgeslist[i]

    edges = set(l)
    G.add_edges_from(edges)

    return(G)


class Graph(BaseTransformParallelized):

    def __init__(self):

        BaseTransformParallelized.__init__(self)
        self.params["nbProcessGraph"] = multiprocessing.cpu_count()

    def computeSnapshot(self, t, routes, updatesParsed):
        self.pq.waitUntilFree()
        G = buildGraphParallelized(routes, self.params["nbProcessGraph"])
        self.pq.addProcess(target=self.runTransforms, args=(self.data, t, G))
        if(t%self.params["nbProcess"]==self.params["nbProcess"]-1):
            self.pq.runOnce()

    def runTransforms(self, data, index, G):
        data[index] = self.transforms(index, G)
        del G
        return(None)

    def transforms(self, index, G):
        
        edgelist = [line for line in nx.generate_edgelist(G)]

        return(edgelist)


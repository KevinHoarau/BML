from BML.transform.graph import Graph
from BML.utils import ProcessingQueue, timeFormat
import multiprocessing

import networkit as nk
import networkx as nx
import numpy as np

import random

import time

def nx2cu(G):
    import cugraph, cudf
    edges = [(int(a),int(b)) for a,b in [*G.edges]]
    edgelistDF = cudf.DataFrame(edges, columns=['src','dst'])
    Gcu = cugraph.from_cudf_edgelist(edgelistDF, source='src', destination='dst', renumber=True)
    return(Gcu)

def nx2nk(G):
    Gnk = nk.nxadapter.nx2nk(G)
    Gnk.indexEdges()
    return(Gnk)

def dictKeys(d, keys):
    subD = {}
    for k in keys:
        subD[k] = d[k]
    return(subD)

def valuesDict(values, keys):
    return(dict(zip(keys, values)))

def betweenness(G, nodes):
    d = nx.betweenness_centrality(G)
    return(dictKeys(d,nodes))

def betweenness_nk(G, Gnk, nodes):
    d = nk.centrality.Betweenness(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def betweenness_subset(G, nodes):
    d = nx.betweenness_centrality_subset(G, nodes, G.nodes)
    return(dictKeys(d,nodes))

def approx_betweenness_nk(G, Gnk, nodes):
    d = nk.centrality.EstimateBetweenness(Gnk, len(nodes)).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def load(G, nodes):
    v = [nx.load_centrality(G, v=n) for n in nodes]
    return(valuesDict(v, nodes))

def closeness(G, nodes):
    v = [nx.closeness_centrality(G, u=n) for n in nodes]
    return(valuesDict(v, nodes))

def closeness_nk(G, Gnk, nodes):
    d = nk.centrality.Closeness(Gnk, False, False).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def approx_closeness_nk(G, Gnk, nodes):
    d = nk.centrality.ApproxCloseness(Gnk, len(nodes)).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def harmonic(G, nodes):
    d = nx.harmonic_centrality(G, nbunch=nodes)
    return(d)

def harmonic_nk(G, Gnk, nodes):
    d = nk.centrality.HarmonicCloseness(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def degree(G, nodes):
    d = G.degree
    return(dictKeys(d,nodes))

def degree_centrality(G, nodes):
    d = nx.degree_centrality(G)
    return(dictKeys(d,nodes))

def degree_centrality_nk(G, Gnk, nodes):
    d = nk.centrality.DegreeCentrality(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def eigenvector(G, nodes):
    d = nx.eigenvector_centrality(G)
    return(dictKeys(d,nodes))

def eigenvector_nk(G, Gnk, nodes):
    d = nk.centrality.EigenvectorCentrality(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def pagerank(G, nodes):
    d = nx.pagerank(G)
    return(dictKeys(d,nodes))

def pagerank_nk(G, Gnk, nodes):
    d = nk.centrality.PageRank(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def number_of_cliques(G, nodes):
    d = nx.number_of_cliques(G, nodes=nodes)
    return(dictKeys(d,nodes))

def number_of_cliques_nk(G, Gnk, nodes):
    cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
    d = {}
    for n,v in zip(G.nodes, Gnk.iterNodes()):
        if(n in nodes):
            d[n] = len([1 for c in cliques if v in c])
    return(d)

def node_clique_number(G, nodes):
    d = nx.node_clique_number(G, nodes=nodes)
    return(dictKeys(d,nodes))

def node_clique_number_nk(G, Gnk, nodes, cliques=None):
    cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
    d = {}
    for n,v in zip(G.nodes, Gnk.iterNodes()):
        if(n in nodes):
            d[n] = max([len(c) for c in cliques if v in c])
    return(d)

def clustering(G, nodes):
    d = nx.clustering(G, nodes=nodes)
    return(dictKeys(d,nodes))

def clustering_nk(G, Gnk, nodes):
    d = nk.centrality.LocalClusteringCoefficient(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def triangles(G, nodes):
    d = nx.triangles(G, nodes=nodes)
    return(dictKeys(d,nodes))

def triangles_nk(G, Gnk, nodes):
    d = nk.sparsification.TriangleEdgeScore(Gnk).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def square_clustering(G, nodes):
    d = nx.square_clustering(G, nodes=nodes)
    return(dictKeys(d,nodes))

def average_neighbor_degree(G, nodes):
    d = nx.average_neighbor_degree(G, nodes=nodes)
    return(dictKeys(d,nodes))

def eccentricity(G, nodes):
    v = [nx.eccentricity(G, v=n) for n in nodes]
    return(valuesDict(v, nodes))

def eccentricity_nk(G, Gnk, nodes):
    d = {}
    for n,v in zip(G.nodes, Gnk.iterNodes()):
        if(n in nodes):
            _,d[n] = nk.distance.Eccentricity.getValue(Gnk,v)
    return(d)

def local_efficiency(G, nodes):
    v = [nx.global_efficiency(G.subgraph(G[n])) for n in nodes]
    return(valuesDict(v, nodes))

def average_shortest_path_length(G, nodes):
    d = nx.single_source_shortest_path_length(G, n)
    return(dictKeys(d,nodes))

def average_shortest_path_length(G, nodes):
    def average_shortest_path_length_node(G, n):
        return(np.mean(list(nx.single_source_shortest_path_length(G,n).values())))
    v = [average_shortest_path_length_node(G, n) for n in nodes]
    return(valuesDict(v, nodes))

def average_shortest_path_length_nk(G, Gnk, nodes):
    def average_shortest_path_length_node(Gnk, n):
        return(np.mean(nk.distance.Dijkstra(Gnk, n).run().getDistances()))
    d = {}
    for n,v in zip(G.nodes, Gnk.iterNodes()):
        if(n in nodes):
            d[n] = average_shortest_path_length_node(Gnk, v)
    return(d)

def connectivity(G, nodes): # too slow, see approx version
    v = []
    for n in nodes:
        v.append(np.mean([nx.connectivity.local_node_connectivity(G,n,t) for t in nodes]))
    return(valuesDict(v, nodes))

def computeFeatures(features, computationTimes=False, verbose=False):
    
    results = {}
    times = {}
    
    for key, value in features.items():
        function, *args = value
        if(verbose):
            print("Start:%s" % (key))
        s = time.time()
        results[key] = function(*args)
        times[key] = time.time() - s
        if(verbose):
            print("End:%s (%s)" % (key, timeFormat(times[key])))

    if(computationTimes):
        results["computation_times"] = times
    
    return(results)


def run(function, results, times, key, verbose,*args):
    if(verbose):
        print("Start:%s" % (key))
    s = time.time()
    results[key] = function(*args)
    times[key] = time.time() - s
    if(verbose):
        print("End:%s (%s)" % (key, timeFormat(times[key])))
    

def computeFeaturesParallelized(features, nbProcess, computationTimes=False, verbose=False):
    
    manager = multiprocessing.Manager()
    results = manager.dict()
    times = manager.dict()
    
    pq = ProcessingQueue(nbProcess=nbProcess)
    
    for key, value in features.items():
        function, *args = value
        pq.addProcess(target=run, args=(function, results, times, key, verbose,*args))
    
    pq.run()
    
    r_copy = {}
    for k in list(features.keys()):
        r_copy[k] = results[k].copy()
        del results[k] 
    
    if(computationTimes):
        r_copy["computation_times2"] = times.copy()
    
    return(r_copy)

def removedExcludedFeatures(features, excluded):
    for key in excluded:
        if(key in features):
            del features[key]
    return(features)


class NodesFeatures(Graph):
    
    def __init__(self):
        Graph.__init__(self)
        self.params["use_networkit"] = True
        self.params["all_nodes"] = False
        self.params["nodes"] = None
        self.params["exclude_features"] = ["load"] # Excluded by default, too slow
        self.params["computation_times"] = False
        self.params["verbose"] = False
    
    
    def transforms(self, index, G):

        nodes = self.params["nodes"]

        if(nodes is None):
            if(not self.params["all_nodes"]):
                core_number = nx.core_number(G)
                k = np.percentile(list(core_number.values()),98)
                G = nx.k_core(G, k, core_number)

            nodes = G.nodes

        results = {}

        features = {}
        features["load"] = (load, G, nodes) # slow
        features["local_efficiency"] = (local_efficiency, G, nodes)
        features["harmonic"] = (harmonic, G, nodes) # slow, but faster than harmonic_nk
        features["degree"] = (degree, G, nodes) # already fast with nx
        features["degree_centrality"] = (degree_centrality, G, nodes) # already fast with nx
        features["square_clustering"] = (square_clustering, G, nodes)
        features["average_neighbor_degree"] = (average_neighbor_degree, G, nodes)
        features["betweenness"] = (betweenness_subset, G, nodes) # faster than nx and nk betweeness

        if(not self.params["use_networkit"]):
            features["closeness"] = (closeness, G, nodes) 
            features["eigenvector"] = (eigenvector, G, nodes)
            features["pagerank"] = (pagerank, G, nodes)
            features["number_of_cliques"] = (number_of_cliques, G, nodes)
            features["node_clique_number"] = (node_clique_number, G, nodes)
            features["clustering"] = (clustering, G, nodes)
            features["triangles"] = (triangles, G, nodes)
            features["eccentricity"] = (eccentricity, G, nodes)
            features["average_shortest_path_length"] = (average_shortest_path_length, G, nodes)

        features = removedExcludedFeatures(features, self.params["exclude_features"])
        results.update(computeFeaturesParallelized(features, self.params["nbProcess"], self.params["computation_times"], self.params["verbose"]))

        if(self.params["use_networkit"]):
            Gnk = nx2nk(G)

            features = {}
            features["closeness"] = (closeness_nk, G, Gnk, nodes)
            features["eigenvector"] = (eigenvector_nk, G, Gnk, nodes)
            features["pagerank"] = (pagerank_nk, G, Gnk, nodes)
            features["number_of_cliques"] = (number_of_cliques_nk, G, Gnk, nodes)
            features["node_clique_number"] = (node_clique_number_nk, G, Gnk, nodes)
            features["clustering"] = (clustering_nk, G, Gnk, nodes)
            features["triangles"] = (triangles_nk, G, Gnk, nodes)
            features["eccentricity"] = (eccentricity_nk, G, Gnk, nodes)
            features["average_shortest_path_length"] = (average_shortest_path_length_nk, G, Gnk, nodes)

            features = removedExcludedFeatures(features, self.params["exclude_features"])
            results.update(computeFeatures(features, self.params["computation_times"], self.params["verbose"]))
        
        return(results)
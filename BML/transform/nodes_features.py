from BML.transform import Graph, BaseTransform
from BML.utils import ProcessingQueue, timeFormat, printAndLog
import multiprocessing

import networkit as nk
import networkx as nx
import numpy as np

import random
import time
import threading

from networkx import find_cliques


def my_node_clique_number(G, nodes=None, cliques=None):
    """Returns the size of the largest maximal clique containing
    each given node.
    Returns a single or list depending on input nodes.
    Optional list of cliques can be input if already computed.
    """
    if cliques is None:
        if nodes is not None:
            # Use ego_graph to decrease size of graph
            if isinstance(nodes, list):
                d = {}
                for n in nodes:
                    H = nx.ego_graph(G, n)
                    d[n] = max(len(c) for c in find_cliques(H))
            else:
                H = nx.ego_graph(G, nodes)
                d = max(len(c) for c in find_cliques(H))
            return d
        # nodes is None--find all cliques
        cliques = list(find_cliques(G))
        
    all_nodes = False
    if nodes is None:
        all_nodes = True
        nodes = list(G.nodes())  # none, get entire graph

    if not isinstance(nodes, list):  # check for a list
        v = nodes
        # assume it is a single value
        d = max([len(c) for c in cliques if v in c])
    else:
        d = {}
        
        for v in nodes:
            d[v] = 0

        for c in cliques:
            l = len(c)
            for v in c:
                if(all_nodes or v in nodes):
                    d[v] = max(d[v],l)
    return d


def my_number_of_cliques(G, nodes=None, cliques=None):
    """Returns the number of maximal cliques for each node.

    Returns a single or list depending on input nodes.
    Optional list of cliques can be input if already computed.
    """
    if cliques is None:
        cliques = list(find_cliques(G))
    
    all_nodes = False
    if nodes is None:
        all_nodes = True
        nodes = list(G.nodes())  # none, get entire graph

    if not isinstance(nodes, list):  # check for a list
        v = nodes
        # assume it is a single value
        numcliq = len([1 for c in cliques if v in c])
    else:
        numcliq = {}
            
        for v in nodes:
            numcliq[v] = 0

        for c in cliques:
            for v in c:
                if(all_nodes or v in nodes):
                    numcliq[v]+=1
        
    return numcliq


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
    keys2 = dict(d).keys()
    for k in keys:
        if(k in keys2):
            subD[k] = d[k]
    return(subD)

def valuesDict(values, keys):
    return(dict(zip(keys, values)))

def betweenness(G, nodes):
    d = nx.betweenness_centrality(G)
    return(dictKeys(d,nodes))

def betweenness_nk(G, Gnk, nodes):
    d = nk.centrality.Betweenness(Gnk,normalized=True).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def load(G, nodes):
    d = nx.load_centrality(G)
    return(dictKeys(d, nodes))

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
    d = nk.centrality.HarmonicCloseness(Gnk, normalized=False).run().scores()
    return(dictKeys(valuesDict(d, G.nodes), nodes))

def degree(G, nodes):
    d = G.degree
    return(dictKeys(d,nodes))

def degree_centrality(G, nodes):
    d = nx.degree_centrality(G)
    return(dictKeys(d,nodes))

def degree_centrality_nk(G, Gnk, nodes):
    d = nk.centrality.DegreeCentrality(Gnk, normalized=True).run().scores()
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
    if(nodes==G.nodes):
        d = my_number_of_cliques(G)
    else:
        d = my_number_of_cliques(G, nodes=list(nodes))
    return(dictKeys(d, nodes))

def number_of_cliques_nk(G, Gnk, nodes):
    cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
    d = {}
    for n,v in zip(G.nodes, Gnk.iterNodes()):
        if(n in nodes):
            d[n] = len([1 for c in cliques if v in c])
    return(d)

def node_clique_number(G, nodes):
    if(nodes==G.nodes):
        d = my_node_clique_number(G)
    else:
        d = my_node_clique_number(G, nodes=list(nodes))
    return(dictKeys(d, nodes))

def node_clique_number_nk(G, Gnk, nodes):
    cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
    v = {}
    for node in Gnk.iterNodes():
        v[node] = 0

    for clique in cliques:
        l = len(clique)
        for node in clique:
            v[node] = max(v[node], l)
    return(dictKeys(valuesDict(v.values(), G.nodes), nodes))

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

def global_efficiency_nk(Gnk):
    n = Gnk.numberOfNodes()
    denom = n * (n - 1)
    if denom != 0:
        g_eff = 0
        lengths = nk.distance.APSP(Gnk).run().getDistances()
        for l in lengths:
            for distance in l:
                if distance > 0:
                    g_eff += 1 / distance
        g_eff /= denom
    else:
        g_eff = 0
    return g_eff

def local_efficiency_nk(G, Gnk, nodes):
    v = [global_efficiency_nk(nx2nk(G.subgraph(G[n]))) for n in nodes]
    return(valuesDict(v, nodes))

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

def run(function, results, key, logFiles, verbose,*args):
    if(verbose):
        printAndLog("Start:%s" % (key), logFiles)
    s = time.time()
    try:
        results[key] = function(*args)
    except Exception as e:
        print("Error with feature: " + key)
        raise e
    if(verbose):
        printAndLog("End:%s (%s)" % (key, timeFormat(time.time()-s)), logFiles)

def computeFeatures(features, logFiles, verbose=False):
    results = {}
    for key, value in features.items():
        function, *args = value
        run(function, results, key, logFiles, verbose,*args)
    return(results)

def computeFeaturesParallelized(features, nbProcess, logFiles, verbose=False):
    
    manager = multiprocessing.Manager()
    results = manager.dict()
    
    pq = ProcessingQueue(nbProcess=nbProcess)
    
    for key, value in features.items():
        function, *args = value
        pq.addProcess(target=run, args=(function, results, key, logFiles, verbose,*args))
    
    pq.run()
    
    r_copy = {}
    for k in list(features.keys()):
        r_copy[k] = results[k].copy()
        del results[k] 
    
    return(r_copy)

def removedExcludedFeatures(features, excluded, included):
    if(len(included)>0):
        for key in list(features.keys()):
            if(not key in included):
                del features[key]
    else:
        for key in excluded:
            if(key in features):
                del features[key]
    return(features)


class NodesFeatures(Graph):
    
    fileExtension = ".json"
    
    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["use_networkit"] = True
        self.params["all_nodes"] = True
        self.params["nodes"] = None
        self.params["exclude_features"] = [] # Excluded by default
        self.params["include_features"] = [
            'degree', 'degree_centrality', 'average_neighbor_degree', 'node_clique_number', 
            'number_of_cliques', 'eigenvector', 'pagerank', 'clustering', 'triangles'
        ]
        self.params["verbose"] = False
        self.params["nbProcessFeatures"] = multiprocessing.cpu_count()

        Graph.__init__(self, primingFile, dataFile, params, outFolder, logFiles)

    def save(self):
        BaseTransform.save(self)
        
    def getNodes(self, G):
        nodes = self.params["nodes"]

        if(nodes is None):
            if(not self.params["all_nodes"]):
                core_number = nx.core_number(G)
                k = np.percentile(list(core_number.values()),98)
                G = nx.k_core(G, k, core_number)

            nodes = G.nodes
            
        return(G, nodes)
            
    def getFeatures(self, G, nodes):
        
        features_nx = {}
        features_nk = {}
        
        features_nx["load"] = (load, G, nodes)
        features_nx["degree"] = (degree, G, nodes) # already fast with nx
        features_nx["degree_centrality"] = (degree_centrality, G, nodes) # already fast with nx
        features_nx["square_clustering"] = (square_clustering, G, nodes)
        features_nx["average_neighbor_degree"] = (average_neighbor_degree, G, nodes)
        features_nx["node_clique_number"] = (node_clique_number, G, nodes) # Fast with my fix
        features_nx["number_of_cliques"] = (number_of_cliques, G, nodes) # Fast with my fix
        
        if(self.params["use_networkit"]):
            Gnk = nx2nk(G)
            features_nk["closeness"] = (closeness_nk, G, Gnk, nodes)
            features_nk["betweenness"] = (betweenness_nk, G, Gnk, nodes)
            features_nk["local_efficiency"] = (local_efficiency_nk, G, Gnk, nodes)
            features_nk["harmonic"] = (harmonic_nk, G, Gnk, nodes) 
            features_nk["eigenvector"] = (eigenvector_nk, G, Gnk, nodes)
            features_nk["pagerank"] = (pagerank_nk, G, Gnk, nodes)
            features_nk["clustering"] = (clustering_nk, G, Gnk, nodes)
            features_nk["triangles"] = (triangles_nk, G, Gnk, nodes)
            features_nk["eccentricity"] = (eccentricity_nk, G, Gnk, nodes)
            features_nk["average_shortest_path_length"] = (average_shortest_path_length_nk, G, Gnk, nodes)
        else:
            features_nx["closeness"] = (closeness, G, nodes) 
            features_nx["betweenness"] = (betweenness, G, nodes)
            features_nx["local_efficiency"] = (local_efficiency, G, nodes)
            features_nx["harmonic"] = (harmonic, G, nodes) 
            features_nx["eigenvector"] = (eigenvector, G, nodes)
            features_nx["pagerank"] = (pagerank, G, nodes)
            features_nx["clustering"] = (clustering, G, nodes)
            features_nx["triangles"] = (triangles, G, nodes)
            features_nx["eccentricity"] = (eccentricity, G, nodes)
            features_nx["average_shortest_path_length"] = (average_shortest_path_length, G, nodes)
            
        return(features_nx, features_nk)
            
    def computeFeatures(self, G, features_nx, features_nk):
        
        results = {}
        
        def func(results,features_nk,verbose):
                results.update(computeFeatures(features_nk, self.logFiles, verbose))    
        thread = threading.Thread(target=func, args=(results, features_nk, self.params["verbose"]))
        thread.start()
        
        results.update(computeFeaturesParallelized(features_nx, self.params["nbProcessFeatures"], self.logFiles, self.params["verbose"]))
        thread.join()
        
        return(results)

    def transforms(self, index, G):
        
        G, nodes = self.getNodes(G)
        
        features_nx, features_nk = self.getFeatures(G, nodes)
        
        features_nx = removedExcludedFeatures(features_nx, self.params["exclude_features"], self.params["include_features"])
        features_nk = removedExcludedFeatures(features_nk, self.params["exclude_features"], self.params["include_features"])

        results = self.computeFeatures(G, features_nx, features_nk)
        
        return(results)
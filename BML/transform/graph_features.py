from BML.transform.graph import Graph
from BML.utils import timeFormat
from BML.transform.nodes_features import *

import multiprocessing
import networkit as nk
import networkx as nx

import random
import time
import numpy as np

def avgNodes(function, *args):
    d = function(*args)
    return(np.mean([*d.values()]))

def nb_of_nodes(G, nodes):
    return(np.float64(len(G.nodes)))

def nb_of_edges(G, nodes):
    return(np.float64(len(G.edges)))

def diameter(G, nodes):
    return(np.float64(nx.diameter(G, usebounds=True)))

def assortativity(G, nodes):
    return(np.float64(nx.degree_assortativity_coefficient(G)))

def percolation_limit(G, nodes):
    degrees = np.array(list(degree(G, nodes).values()))
    k0 = np.sum(degrees/len(G))
    k02 = np.sum((degrees**2)/len(G))
    pl = 1 - 1/(k02/k0 -1)
    return(np.float64(pl))

def node_connectivity(G, nodes): # if ,too slow see approx
    return(np.float64(nx.node_connectivity(G)))

def edge_connectivity(G, nodes):
    return(np.float64(nx.edge_connectivity(G)))

def algebraic_connectivity(G, nodes, eigenvalues=None):
    laplacian_eigenvalues = None
    if(not eigenvalues is None):
        laplacian_eigenvalues = eigenvalues["laplacian"]
    if(laplacian_eigenvalues is None):
        laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
        
    laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
    v = np.min(laplacian_eigenvalues)
    return(np.float64(v))

def largest_eigenvalue(G, nodes, eigenvalues=None):
    adjacency_eigenvalues = None
    if(not eigenvalues is None):
        adjacency_eigenvalues = eigenvalues["adjacency"]
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    return(np.float64(max(adjacency_eigenvalues)))

def symmetry_ratio(G, nodes, eigenvalues=None):
    adjacency_eigenvalues = None
    if(not eigenvalues is None):
        adjacency_eigenvalues = eigenvalues["adjacency"]
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    r = len(np.unique(adjacency_eigenvalues))/(diameter(G, nodes)+1)
    return(np.float64(r))

def natural_connectivity(G, nodes, eigenvalues=None):
    adjacency_eigenvalues = None
    if(not eigenvalues is None):
        adjacency_eigenvalues = eigenvalues["adjacency"]
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    nc = np.log(np.mean(np.exp(adjacency_eigenvalues)))
    return(np.float64(nc))

def weighted_spectrum(G, nodes, n=3, eigenvalues=None):
    normalized_laplacian_eigenvalues = None
    if(not eigenvalues is None):
        normalized_laplacian_eigenvalues = eigenvalues["normalized_laplacian"]
    if(normalized_laplacian_eigenvalues is None):
        normalized_laplacian_eigenvalues = np.real(nx.normalized_laplacian_spectrum(G))
    ws = np.sum((1-normalized_laplacian_eigenvalues)**n)
    return(np.float64(ws))

def effective_graph_resistance(G, nodes, eigenvalues=None):
    laplacian_eigenvalues = None
    if(not eigenvalues is None):
        laplacian_eigenvalues = eigenvalues["laplacian"]
    if(laplacian_eigenvalues is None):
        laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
    laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
    nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
    nst = len(G)*np.sum(1/nonzero_eigenvalues)
    return(np.float64(nst))

def nb_spanning_trees(G, nodes, eigenvalues=None):
    laplacian_eigenvalues = None
    if(not eigenvalues is None):
        laplacian_eigenvalues = eigenvalues["laplacian"]
    if(laplacian_eigenvalues is None):
        laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
    laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
    nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
    nst = np.prod(nonzero_eigenvalues/len(G))
    return(np.float64(nst))

class GraphFeatures(NodesFeatures):
    
    def __init__(self, primingFile, dataFile, params, outFolder, logFiles):
        
        self.params["use_networkit"] = True
        self.params["all_nodes"] = True
        self.params["nodes"] = None
        self.params["exclude_features"] = [] # Excluded by default
        self.params["include_features"] = [
            'degree', 'degree_centrality', 'average_neighbor_degree', 'node_clique_number', 
            'number_of_cliques', 'eigenvector', 'pagerank', 'clustering', 'triangles',
            'nb_of_nodes', 'nb_of_edges', 'diameter', 'assortativity', 'percolation_limit', 
        ]
        self.params["verbose"] = False
        self.params["nbProcessFeatures"] = multiprocessing.cpu_count()

        Graph.__init__(self, primingFile, dataFile, params, outFolder, logFiles)
         
    def getFeatures(self, G, nodes):
        
        features_nx, features_nk = NodesFeatures.getFeatures(self, G, nodes)
        
        for k,v in features_nx.items():
            features_nx[k] = (avgNodes,)+v
        for k,v in features_nk.items():
            features_nk[k] = (avgNodes,)+v
        
        self.eigenvalues={
                "laplacian":None,
                "adjacency":None,
                "normalized_laplacian":None,
        }
        
        features_nx["nb_of_nodes"] = (nb_of_nodes, G, nodes)
        features_nx["nb_of_edges"] = (nb_of_edges, G, nodes)
        features_nx["diameter"] = (diameter, G, nodes)
        features_nx["assortativity"] = (assortativity, G, nodes)
        features_nx["largest_eigenvalue"] = (largest_eigenvalue, G, nodes, self.eigenvalues)
        features_nx["algebraic_connectivity"] = (algebraic_connectivity, G, nodes, self.eigenvalues)
        features_nx["effective_graph_resistance"] = (effective_graph_resistance, G, nodes, self.eigenvalues)
        features_nx["symmetry_ratio"] = (symmetry_ratio, G, nodes, self.eigenvalues)
        features_nx["natural_connectivity"] = (natural_connectivity, G, nodes, self.eigenvalues)
        features_nx["node_connectivity"] = (node_connectivity, G, nodes)
        features_nx["edge_connectivity"] = (edge_connectivity, G, nodes)
        features_nx["weighted_spectrum_3"] = (weighted_spectrum, G, nodes, 3, self.eigenvalues)
        features_nx["weighted_spectrum_4"] = (weighted_spectrum, G, nodes, 4, self.eigenvalues)
        features_nx["percolation_limit"] = (percolation_limit, G, nodes)
        features_nx["nb_spanning_trees"] = (nb_spanning_trees, G, nodes, self.eigenvalues)
        
        return(features_nx, features_nk)
            
    def computeFeatures(self, G, features_nx, features_nk):
    
        if("effective_graph_resistance" in features_nx or "nb_spanning_trees" in features_nx or "algebraic_connectivity" in features_nx):
            if(self.params["verbose"]):
                print("Computing laplacian_eigenvalues")
                s = time.time()
            self.eigenvalues["laplacian"] = np.real(nx.laplacian_spectrum(G))
            if(self.params["verbose"]):
                print("Finish laplacian_eigenvalues (%s)" % (timeFormat(time.time()-s)))
                
        if("largest_eigenvalue" in features_nx or "symmetry_ratio" in features_nx or "natural_connectivity" in features_nx):
            if(self.params["verbose"]):
                print("Computing adjacency_eigenvalues")
                s = time.time()
            self.eigenvalues["adjacency"] = np.real(nx.adjacency_spectrum(G))
            if(self.params["verbose"]):
                print("Finish adjacency_eigenvalues (%s)" % (timeFormat(time.time()-s)))
                
        if("weighted_spectrum_3" in features_nx or "weighted_spectrum_4" in features_nx):
            if(self.params["verbose"]):
                print("Computing normalized_laplacian_eigenvalues")
                s = time.time()
            self.eigenvalues["normalized_laplacian"] = np.real(nx.normalized_laplacian_spectrum(G))
            if(self.params["verbose"]):
                print("Finish normalized_laplacian_eigenvalues (%s)" % (timeFormat(time.time()-s)))
        
        return(NodesFeatures.computeFeatures(self, G, features_nx, features_nk))

    
from BML.transform.graph import Graph
from BML.utils import timeFormat
from BML.transform.nodes_features import *

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

def largest_eigenvalue(G, nodes, adjacency_eigenvalues=None):
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    return(np.float64(max(adjacency_eigenvalues)))

def algebraic_connectivity(G, nodes):
    ac = nx.algebraic_connectivity(G)
    return(np.float64(ac))

def symmetry_ratio(G, nodes, adjacency_eigenvalues=None):
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    r = len(np.unique(adjacency_eigenvalues))/(diameter(G, nodes)+1)
    return(np.float64(r))

def natural_connectivity(G, nodes, adjacency_eigenvalues=None):
    if(adjacency_eigenvalues is None):
        adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
    nc = np.log(np.mean(np.exp(adjacency_eigenvalues)))
    return(np.float64(nc))

def weighted_spectrum(G, nodes, normalized_laplacian_eigenvalues=None, n=3):
    if(normalized_laplacian_eigenvalues is None):
        normalized_laplacian_eigenvalues = np.real(nx.normalized_laplacian_spectrum(G))
    ws = np.sum((1-normalized_laplacian_eigenvalues)**n)
    return(np.float64(ws))

def percolation_limit(G, nodes, n=3):
    degrees = np.array(list(degree(G, nodes).values()))
    k0 = np.sum(degrees)/len(G)
    k02 = np.sum(degrees**2)/len(G)
    pl = 1 - 1/(k02/k0 -1)
    return(np.float64(pl))

def effective_graph_resistance(G, nodes, laplacian_eigenvalues=None):
    if(laplacian_eigenvalues is None):
        laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
    nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
    nst = len(G)*np.sum(1/nonzero_eigenvalues)
    return(np.float64(nst))

def node_connectivity(G, nodes): # if ,too slow see approx
    return(np.float64(nx.node_connectivity(G)))

def edge_connectivity(G, nodes):
    return(np.float64(nx.edge_connectivity(G)))

def nb_spanning_trees(G, nodes, laplacian_eigenvalues=None):
    if(laplacian_eigenvalues is None):
        laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
    nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
    nst = np.prod(nonzero_eigenvalues/len(G))
    return(np.float64(nst))

class GraphFeatures(Graph):
    
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

        laplacian_eigenvalues = None
        adjacency_eigenvalues = None
        normalized_laplacian_eigenvalues = None
        if(not "effective_graph_resistance" in self.params["exclude_features"] or not "nb_spanning_trees" in self.params["exclude_features"]):
            if(self.params["verbose"]):
                print("Computing laplacian_eigenvalues")
            s = time.time()
            laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
            if(self.params["verbose"]):
                print("Finish laplacian_eigenvalues (%s)" % (timeFormat(time.time()-s)))
        if(not "largest_eigenvalue" in self.params["exclude_features"] or not "symmetry_ratio" in self.params["exclude_features"] or not "natural_connectivity" in self.params["exclude_features"]):
            if(self.params["verbose"]):
                print("Computing adjacency_eigenvalues")
            s = time.time()
            adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
            if(self.params["verbose"]):
                print("Finish adjacency_eigenvalues (%s)" % (timeFormat(time.time()-s)))
        if(not "weighted_spectrum_3" in self.params["exclude_features"] or not "weighted_spectrum_4" in self.params["exclude_features"]):
            if(self.params["verbose"]):
                print("Computing normalized_laplacian_eigenvalues")
            s = time.time()
            normalized_laplacian_eigenvalues = np.real(nx.normalized_laplacian_spectrum(G))
            if(self.params["verbose"]):
                print("Finish normalized_laplacian_eigenvalues (%s)" % (timeFormat(time.time()-s)))

        results = {}

        features = {}
        features["nb_of_nodes"] = (nb_of_nodes, G, nodes)
        features["nb_of_edges"] = (nb_of_edges, G, nodes)
        features["diameter"] = (diameter, G, nodes)
        features["assortativity"] = (assortativity, G, nodes)
        features["largest_eigenvalue"] = (largest_eigenvalue, G, nodes, adjacency_eigenvalues)
        features["algebraic_connectivity"] = (algebraic_connectivity, G, nodes)
        features["effective_graph_resistance"] = (effective_graph_resistance, G, nodes, laplacian_eigenvalues)
        features["symmetry_ratio"] = (symmetry_ratio, G, nodes, adjacency_eigenvalues)
        features["natural_connectivity"] = (natural_connectivity, G, nodes, adjacency_eigenvalues)
        features["node_connectivity"] = (node_connectivity, G, nodes)
        features["edge_connectivity"] = (edge_connectivity, G, nodes)
        features["weighted_spectrum_3"] = (weighted_spectrum, G, nodes, normalized_laplacian_eigenvalues, 3)
        features["weighted_spectrum_4"] = (weighted_spectrum, G, nodes, normalized_laplacian_eigenvalues, 4)
        features["percolation_limit"] = (percolation_limit, G, nodes)
        features["nb_spanning_trees"] = (nb_spanning_trees, G, nodes, laplacian_eigenvalues)

        features["load"] = (avgNodes, load, G, nodes) # slow
        features["local_efficiency"] = (avgNodes, local_efficiency, G, nodes)
        features["harmonic"] = (avgNodes, harmonic, G, nodes) # slow, but faster than harmonic_nk
        features["degree"] = (avgNodes, degree, G, nodes) # already fast with nx
        features["degree_centrality"] = (avgNodes, degree_centrality, G, nodes) # already fast with nx
        features["square_clustering"] = (avgNodes, square_clustering, G, nodes)
        features["average_neighbor_degree"] = (avgNodes, average_neighbor_degree, G, nodes)

        if(not self.params["use_networkit"]):
            features["betweenness"] = (avgNodes, betweenness, G, nodes)
            features["closeness"] = (avgNodes, closeness, G, nodes) 
            features["eigenvector"] = (avgNodes, eigenvector, G, nodes)
            features["pagerank"] = (avgNodes, pagerank, G, nodes)
            features["number_of_cliques"] = (avgNodes, number_of_cliques, G, nodes)
            features["node_clique_number"] = (avgNodes, node_clique_number, G, nodes)
            features["clustering"] = (avgNodes, clustering, G, nodes)
            features["triangles"] = (avgNodes, triangles, G, nodes)
            features["eccentricity"] = (avgNodes, eccentricity, G, nodes)
            features["average_shortest_path_length"] = (avgNodes, average_shortest_path_length, G, nodes)

        features = removedExcludedFeatures(features, self.params["exclude_features"])
        results.update(computeFeaturesParallelized(features, self.params["nbProcess"], self.params["computation_times"], self.params["verbose"]))

        del laplacian_eigenvalues
        del adjacency_eigenvalues
        del normalized_laplacian_eigenvalues
        
        if(self.params["use_networkit"]):
            Gnk = nx2nk(G)

            features = {}
            features["betweenness"] = (avgNodes, betweenness_nk, G, Gnk, nodes)
            features["closeness"] = (avgNodes, closeness_nk, G, Gnk, nodes)
            features["eigenvector"] = (avgNodes, eigenvector_nk, G, Gnk, nodes)
            features["pagerank"] = (avgNodes, pagerank_nk, G, Gnk, nodes)
            features["number_of_cliques"] = (avgNodes, number_of_cliques_nk, G, Gnk, nodes)
            features["node_clique_number"] = (avgNodes, node_clique_number_nk, G, Gnk, nodes)
            features["clustering"] = (avgNodes, clustering_nk, G, Gnk, nodes)
            features["triangles"] = (avgNodes, triangles_nk, G, Gnk, nodes)
            features["eccentricity"] = (avgNodes, eccentricity_nk, G, Gnk, nodes)
            features["average_shortest_path_length"] = (avgNodes, average_shortest_path_length_nk, G, Gnk, nodes)

            features = removedExcludedFeatures(features, self.params["exclude_features"])
            results.update(computeFeatures(features, self.params["computation_times"], self.params["verbose"]))
        
        return(results)

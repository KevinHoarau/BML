from BML.transform import BaseTransform

class RoutesFeatures(BaseTransform):
    
    computeUpdates = True

    def transforms(self, index, routes, updates):

        nb_prefixes = 0
        nb_routes = 0

        ASes = set()
        links = set()


        for prefix in routes.keys():

            nb_prefixes += 1

            for collector in routes[prefix].keys():

                for peer in routes[prefix][collector].keys():

                    nb_routes += 1

                    path = routes[prefix][collector][peer]
                    path_vertices = []
                    path_edges = []

                    if(path != None):

                        if('{' in path or '}' in path):
                            pass
                        else:
                            path_vertices = path.split(' ')
                            for i in range(len(path_vertices)-1):
                                path_edges.append([path_vertices[i], path_vertices[i+1]])


                        for AS in path_vertices:
                            ASes.add(AS)

                        for edge in path_edges:
                            links.add(str(edge))

        features = {
            "nb_prefixes":nb_prefixes,
            "nb_routes":nb_routes,
            "nb_ASes":len(ASes),
            "nb_links":len(links),
        }


        return(features)

from BML.transform import BaseTransformParallelized

class HijackLabel(BaseTransformParallelized):

    def __init__(self):
        BaseTransformParallelized.__init__(self)
        self.params["hijack"] = None

    def transforms(self, index, routes, updates):

        label = {"Y":0}

        if(not self.params["hijack"] is None):

            hj_prefix = self.params["hijack"]["prefix"]
            hj_type = self.params["hijack"]["type"]
            hj_as = self.params["hijack"]["hijack_as"]

            if(hj_prefix in routes):
                for collector in routes[hj_prefix].keys():
                    for peer in routes[hj_prefix][collector].keys():

                        if(not routes[hj_prefix][collector][peer] is None):

                            path = routes[hj_prefix][collector][peer].split(" ")

                            if(hj_type=="origin_change"):

                                if(str(path[-1])==str(hj_as)):
                                    label = {"Y":1}

                            if(hj_type=="forged_as_path"):

                                if(str(hj_as) in path):
                                    label = {"Y":1}

        return(label)

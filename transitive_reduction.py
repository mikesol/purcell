import pprint

class Graph:
    nodes = []
    edges = []
    removed_edges = []
    def remove_edge(self,x,y):
        e = (x,y)
        try:
            self.edges.remove(e)
            #print("Removed edge %s" % str(e))
            self.removed_edges.append(e)
        except:
            pass
            #print("Attempted to remove edge %s, but it wasn't there" % str(e))

    def Nodes(self):
        return self.nodes

    # Sample data
    def __init__(self):
        self.nodes = [1,2,3,4,5]
        self.edges = [
            (1,2),
            (1,3),
            (1,4),
            (1,5),
            (2,4),
            (3,4),
            (3,5),
            (4,5),
        ]


def do_transitive_reduction_of_graph(graph) :
    N = graph.Nodes()
    for x in N:
       for y in N:
          for z in N:
             #print("(%d,%d,%d)" % (x,y,z))
             if (x,y) != (y,z) and (x,y) != (x,z):
                if (x,y) in graph.edges and (y,z) in graph.edges:
                    graph.remove_edge(x,z)

if __name__ == "__main__" :
    G = Graph()
    do_transitive_reduction_of_graph(G)
    print("Removed edges:")
    pprint.pprint(G.removed_edges)
    print("Remaining edges:")
    pprint.pprint(G.edges)
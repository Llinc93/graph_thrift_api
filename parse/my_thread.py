import threading, time

from model.ent_graph import neo4j_client


class MyThread(threading.Thread):

    def __init__(self, ent_names, level, nodes, links, filter, direct):
        threading.Thread.__init__(self)
        self.ent_names = ent_names
        self.level = int(level)
        self.nodes = nodes
        self.links = links
        self.filter = filter
        self.direct = direct
        self.result = None

    def run(self):
        data = neo4j_client.get_ents_relevance_seek_graph_g_v3(entnames=self.ent_names, level=self.level, terms=(self.nodes, self.links, self.direct))
        if data:
            self.result = data
        return None


class MyThreadAPOC(threading.Thread):

    def __init__(self, index, entNames, level, relationshhipFilter):
        super(MyThreadAPOC, self).__init__()
        self.entname = entNames[index]
        self.entnames = [i for i in entNames if i != entNames[index]]
        self.level = level
        self.relationshipFilter = relationshhipFilter
        self.result = None

    def filter_graph(self, graph):
        data = []
        for path in graph:
            nodes = path['n']
            links = path['r']
            if nodes[0]['NAME'] != self.entname or nodes[-1]['NAME'] not in self.entnames:
                continue

            data.append({'n': nodes, 'r':links})
        return data

    def run(self):
        data = neo4j_client.get_ent_graph_g_v4(entname=self.entname, level=self.level, node_type='GS', relationshipFilter=self.relationshipFilter)
        self.result = self.filter_graph(data)
        return None

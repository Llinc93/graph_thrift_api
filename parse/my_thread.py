import threading, time

from model.ent_graph import neo4j_client
from parse.parse_graph import parse


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
        self.result = parse.parse_v3(data, self.filter, self.level, self.ent_names[-1])
        return None
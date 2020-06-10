import time, redis, re
from py2neo import Graph

import config


class RedisClient(object):
    def __init__(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
        self.r = redis.Redis(connection_pool=pool)


class Neo4jClient(object):

    def __init__(self):
        self.graph = Graph(config.NEO4J_URL, username=config.NEO4J_USER, password=config.NEO4J_PASSWD)

        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)

    def get_level(self, lcid):
        """
        获取查询层级
        :param lcid:
        :return:
        """
        try:
            if self.r.exists(lcid):
                level = int(self.r.get(lcid)) + 1
            else:
                level = 3
        except Exception:
            level = 10
        return level

    def get_lcid(self, entname, usccode):
        """
        获取企业ID
        :param entname:
        :param usccode:
        :return:
        """
        if entname:
            command = "match (n:GS {NAME: '%s'}) return n.ID as lcid"
            rs = self.graph.run(command % entname).data()
        else:
            command = "match (n:GS {UNISCID: '%s'}) return n.ID as lcid"
            rs = self.graph.run(command % usccode).data()
        lcid = rs[0] if rs else ''
        return lcid['lcid'] if lcid else ''

    def get_ent_actual_controller(self, entname, usccode, level):
        """
        企业实际控制人接口
        :param entname:
        :param usccode:
        :param level:
        :return:
        """
        if entname:
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {NAME: '%s'}) return " \
                      "distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, entname))
            print(command % (level, entname))
        else:
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {UNISCID: '%s'}) return " \
                      "distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, usccode))
        
        info = rs.data()
        rs.close()
        return info

    def get_final_beneficiary_name(self, lcid, level):
        """
        企业最终控制人接口
        :param entname:
        :param usccode:
        :param level:
        :return:
        """
        command = "match p = (n) -[r:TIPEES* 1 .. %s]-> (m:GS {ID: '%s'}) return distinct " \
                  "[node in nodes(p) | properties(node)] as n, [link in relationships(p) | properties(link)] as r"
        rs = self.graph.run(command % (level, lcid))
        info = rs.data()
        rs.close()
        return info

    def get_ent_graph_g(self, entname, level, node_type, relationship_filter):
        """
        企业族谱
        :param entname:
        :param level:
        :param node_type:
        :param relationship_filter:
        :return:
        """
        if node_type != 'GS':
            node_attribute = 'ID'
        else:
            if len(entname) == 32 and re.findall('[a-z0-9]', entname):
                node_attribute = 'ID'
            else:
                node_attribute = 'NAME'
        flag = True
        command = "match (n:%s {%s: '%s'}) call apoc.path.expand(n, '%s', '', 1, %s) yield path return " \
                  "[n in nodes(path) | properties(n)] as n, [r in relationships(path) | properties(r)] as r"
        print(command % (node_type, node_attribute, entname, relationship_filter, level))
        rs = self.graph.run(command % (node_type, node_attribute, entname, relationship_filter, level))
        info = rs.data()
        if not info:
            flag = False
        rs.close()
        return info, flag

    def get_ent_relevance_seek_graph(self, entnames, level, relationship_filter):
        """
        关联探寻
        :param entnames:
        :param level:
        :param relationship_filter:
        :return:
        """
        flag = True
        command = "MATCH (p:GS {NAME: '%s'}) MATCH (end:GS) WHERE end.NAME IN %s " \
                  "WITH p, collect(end) AS endNodes " \
                  "CALL apoc.path.expandConfig(p, {relationshipFilter: '%s', minLevel: 1, maxLevel: %s, " \
                  "endNodes: endNodes}) YIELD path RETURN nodes(path) as n, relationships(path) as r"
        print(command % (entnames[0], entnames[1:], relationship_filter, level))
        rs = self.graph.run(command % (entnames[0], entnames[1:], relationship_filter, level))
        info = rs.data()
        if not info:
            flag = False
        rs.close()
        return info, flag


neo4j_client = Neo4jClient()

redis_client = RedisClient()

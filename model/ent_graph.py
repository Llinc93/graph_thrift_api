import time, redis, re
from py2neo import Graph

import config


class RedisClient(object):
    def __init__(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
        self.r = redis.Redis(connection_pool=pool)


class Neo4jClient(object):

    def __init__(self):
        self.graph = Graph(config.NEO4J_URL, username=config.NEO4J_USER, password =config.NEO4J_PASSWD)

        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)

    def get_level(self, lcid):
        '''
        获取查询层级
        :param lcid:
        :return:
        '''
        try:
            if self.r.exists(lcid):
                level = int(self.r.get(lcid)) + 1
            else:
                level = 3
        except:
            level = 10
        return level

    def get_lcid(self, entname, usccode):
        '''
        获取企业ID
        :param entname:
        :param usccode:
        :return:
        '''
        if entname:
            command = "match (n:GS {NAME: '%s'}) return n.ID as lcid"
            rs = self.graph.run(command % entname).data()
        else:
            command = "match (n:GS {UNISCID: '%s'}) return n.ID as lcid"
            rs = self.graph.run(command % usccode).data()
        lcid = rs[0] if rs else ''
        return lcid['lcid'] if lcid else ''

    def get_ent_actual_controller(self, entname, usccode, level):
        '''
        企业实际控制人接口
        :param entname:
        :param usccode:
        :return:
        '''
        if entname:
            # command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {NAME: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {NAME: '%s'}) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, entname))
            print(command % (level, entname))
        else:
            # command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {UNISCID: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {UNISCID: '%s'}) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, usccode))
        
        info = rs.data()
        rs.close()
        return info

    def get_final_beneficiary_name(self, entname, usccode, level):
        '''
        企业实际控制人接口
        :param entname:
        :param usccode:
        :return:
        '''
        if entname:
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {NAME: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, entname))
        else:
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {UNISCID: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, usccode))
        info = rs.data()
        rs.close()
        return info

    def get_ent_graph_g(self, entname, level, node_type, relationshipFilter):
        '''
        企业族谱
            match (n:{node_type} {NAME: '{entname}'}) call apoc.path.expand(n, '<IPEE|BEE|<SPE', '', 1, {level}) yield path
             foreach(n in nodes(path) | set n.extendnumber=size((n) -[]-> ()))
             foreach(r in relationships(path) | set r.start_id=properties(startNode(r))['ID'])
             foreach(r in relationships(path) | set r.end_id=properties(endNode(r))['ID'])
             foreach(r in relationships(path) | set r.labe=type(r))
             foreach(n in nodes(path) | set n.label=labels(n)[0])
             return [n in nodes(path) | properties(n)], [r in relationships(path) | properties(r)]
        :param entname:
        :param level:
        :return:
        '''
        if node_type != 'GS':
            node_attribute = 'ID'
        else:
            if len(entname) == 32 and re.findall('[a-z0-9]', entname):
                node_attribute = 'ID'
            else:
                node_attribute = 'NAME'
        flag = True
        # command = "match (n:%s {%s: '%s'}) call apoc.path.expand(n, '%s', '', 1, %s) yield path foreach(r in relationships(path) | set r.pid=properties(startNode(r))['ID']) foreach(r in relationships(path) | set r.id=properties(endNode(r))['ID']) foreach(r in relationships(path) | set r.label=type(r)) foreach(r in relationships(path) | set r.ID=id(r)) foreach(n in nodes(path) | set n.label=labels(n)[0]) return [n in nodes(path) | properties(n)] as n, [r in relationships(path) | properties(r)] as r"
        command = "match (n:%s {%s: '%s'}) call apoc.path.expand(n, '%s', '', 1, %s) yield path return [n in nodes(path) | properties(n)] as n, [r in relationships(path) | properties(r)] as r"
        print(command % (node_type, node_attribute, entname, relationshipFilter, level))
        rs = self.graph.run(command % (node_type, node_attribute, entname, relationshipFilter, level))
        info = rs.data()
        if not info:
            flag = False
        rs.close()
        return info, flag

    def get_ent_relevance_seek_graph(self, entnames, level, relationshipFilter):
        '''
        关联探寻
        :param entname:
        :param level:
        :param relationshipFilter:
        :return:
        '''
        flag = True
        command = "MATCH (p:GS {NAME: '%s'}) MATCH (end:GS) WHERE end.NAME IN %s WITH p, collect(end) AS endNodes CALL apoc.path.expandConfig(p, {relationshipFilter: '%s', minLevel: 1, maxLevel: %s, endNodes: endNodes}) YIELD path RETURN nodes(path) as n, relationships(path) as r"
        print(command % (entnames[0], entnames[1:], relationshipFilter, level))
        rs = self.graph.run(command % (entnames[0], entnames[1:], relationshipFilter, level))
        info = rs.data()
        if not info:
            flag = False
        rs.close()
        return info, flag

    # def get_extendnumber(self, entnames, relationshipFilter):
    #     command = "match (n) where n.ID in %s call apoc.neighbors.byhop.count(n, '%s', 1) yield value return value"
    #     rs = self.graph.run(command % (entnames, relationshipFilter))
    #     info = rs.data()
    #     rs.close()
    #     return info

    def get_extendNumber(self, node, relationshipFilter):
        command = "match (n:%s {ID: '%s'}) call apoc.neighbors.athop.count(n, '%s', 1) yield value return value"
        rs = self.graph.run(command % (node['label'], node['ID'], relationshipFilter))
        info = rs.data()
        rs.close()
        return info


neo4j_client = Neo4jClient()

import time, redis
from py2neo import Graph

import config


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
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {NAME: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            rs = self.graph.run(command % (level, entname))
        else:
            command = "match p = (n) -[r:IPEER|:IPEES|:BEE* 1 .. %s]-> (m:GS {UNISCID: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0]) foreach(link in relationships(p) | set link.ID=id(link)) foreach(link in relationships(p) | set link.label=type(link)) return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
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


    def get_ent_graph_g_v4(self, entname, level, node_type, relationshipFilter):
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
        flag = True
        command = "match (n:%s {NAME: '%s'}) call apoc.path.expand(n, '%s', '', 1, %s) yield path foreach(r in relationships(path) | set r.pid=properties(startNode(r))['ID']) foreach(r in relationships(path) | set r.id=properties(endNode(r))['ID']) foreach(r in relationships(path) | set r.label=type(r)) foreach(r in relationships(path) | set r.ID=id(r)) foreach(n in nodes(path) | set n.label=labels(n)[0]) return [n in nodes(path) | properties(n)] as n, [r in relationships(path) | properties(r)] as r"
        print(command % (node_type, entname, relationshipFilter, level))
        rs = self.graph.run(command % (node_type, entname, relationshipFilter, level))
        info = rs.data()
        if not info:
            flag = False
        rs.close()
        return info, flag

    def get_ents_relevance_seek_graph_g_v3(self, entnames, level, terms):
        '''
        多节点关系查询
        :param entnames:
        :param level:
        :param terms:
        :return:
        '''
        nodes_type, links_type, direction = terms
        start = "match p = (n:GS {NAME: '%s'})"

        if direction == 'full':
            relationship = ' -[r{}* .. %s]-'
        elif direction == 'out':
            relationship = ' <-[r{}* .. %s]-'
        else:
            relationship = ' -[r{}* .. %s]->'

        if len(links_type) == 1:
            link_term = ':{}'.format(links_type[0])
        elif links_type:
            link_term = ':{}'.format(' | '.join(links_type))
        else:
            link_term = ''
        relationship = relationship.format(link_term)

        end = " (m:GS {NAME: '%s'})"

        if len(nodes_type) == 1:
            label = ' where n:{} '.format(nodes_type[0])
        elif nodes_type:
            tmp = []
            for n in nodes_type:
                tmp.append('n:{}'.format(n))
            label_term = ' or '.join(tmp)
            label = ' where ' + label_term
        else:
            label = ' '

        tail = " foreach(r in relationships(p) | set r.pid=properties(startNode(r))['ID']) foreach(r in relationships(p) | set r.id=properties(endNode(r))['ID']) foreach(r in relationships(p) | set r.label=type(r)) foreach(r in relationships(p) | set r.ID=id(r)) foreach(n in nodes(p) | set n.label=labels(n)[0]) return [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
        command = start + relationship + end + label + tail

        print(command % (entnames[0], level, entnames[1]))
        rs = self.graph.run(command % (entnames[0], level, entnames[1]))
        info = rs.data()
        rs.close()
        return info

    def get_extendnumber(self, entnames, relationshipFilter):
        command = "match (n:GS) where n.NAME in %s call apoc.neighbors.byhop.count(n, '%s', 1) yield value return value"
        rs = self.graph.run(command % (entnames, relationshipFilter))
        info = rs.data()
        rs.close()
        return info


neo4j_client = Neo4jClient()


if __name__ == '__main__':
    import time

    ent = ['江苏荣马城市建设有限公司', '江苏臻天机科技有限公司', '南京晨光集团有限公司', '江苏建科建设监理有限公司', '苏州勇德云服饰有限公司']
    for i in ent:
        s1 = time.time()
        command1 = "match p = () -[:IPEES|:IPEER|:BEE|:SPE* 1 .. 6]- (n:GS {NAME: '%s'}) return p"
        neo4j_client.graph.run(command1 % i)
        ret1 = time.time() - s1
        print(f'cypher:\t{i}\t', ret1)

        s2 = time.time()
        command2 = "match (n:GS {NAME: '%s'}) call apoc.path.expand(n, 'IPEER|IPEES|BEE|SPE', '', 1, 6) yield path return path"
        neo4j_client.graph.run(command2 % i)
        ret2 = time.time() - s2
        print(f'cypher:\t{i}\t', ret2)

        print(ret1-ret2)
        print()

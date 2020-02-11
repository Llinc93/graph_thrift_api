import time
from py2neo import Graph

import config


class Neo4jClient(object):

    def __init__(self):
        start = time.time()
        self.graph = Graph(config.NEO4J_URL, username=config.NEO4J_USER, password =config.NEO4J_PASSWD)
        print(time.time() - start)

    def example(self, invname):
        """
        获取节点状态
        """
        command = "match (n:inv {INVNAME:'%s'}) return n.INVNAME as INVNAME, n.INVNAME_GLLZD AS INVNAME_GLLZD"
        rs = self.graph.run(command % invname)
        node_status = rs.data()
        rs.close()
        return node_status

    def get_ent_actual_controller(self, entname, usccode):
        '''
        企业实际控制人接口
        :param entname:
        :param usccode:
        :return:
        '''
        if usccode:
            # command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {UNISCID: '%s'}) return properties(n) as snode, labels(n) as snode_type, r as links"
            command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {UNISCID: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0])  return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            print(command % usccode)
            rs = self.graph.run(command % usccode)
        else:
            # command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {NAME: '%s'}) return properties(n) as snode, labels(n) as snode_type, r as links"
            command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {NAME: '%s'}) foreach(n in nodes(p) | set n.label=labels(n)[0])  return distinct [n in nodes(p) | properties(n)] as n, [r in relationships(p) | properties(r)] as r"
            print(command % entname)
            rs = self.graph.run(command % entname)
        info = rs.data()
        rs.close()
        return info

    def get_ent_graph_g(self, entname, level, node_type, terms):
        '''
        企业族谱
        :param entname:
        :param level:
        :return:
        '''
        print(terms)
        nodes_type, links_type, direction = terms
        start = "match p = (n)"

        if direction == 0:
            relationship = ' -[r{}* .. %s]-'
        elif direction == 1:
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

        end = " (m:%s {NAME: '%s'})"

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

        tail = ' return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links'
        command = start + relationship + end + label + tail
        print(command % (level, node_type, entname))
        rs = self.graph.run(command % (level, node_type, entname))
        info = rs.data()
        rs.close()
        return info

    def get_ents_relevance_seek_graph_g(self, entnames, level, terms):
        '''
        企业关联
        :return:
        '''
        command = "match p= (n:GS {NAME: '%s'}) -[r* .. %s]- (m:GS {NAME: '%s'}) return p"

        print(terms)
        nodes_type, links_type, direction = terms
        start = "match p = (n:GS {NAME: '%s'})"

        if direction == 0:
            relationship = ' -[r{}* .. %s]-'
        elif direction == 1:
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

        tail = ' return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links'
        command = start + relationship + end + label + tail

        print(command % (entnames[0], level, entnames[1]))
        rs = self.graph.run(command % (entnames[0], level, entnames[1]))
        info = rs.data()
        rs.close()
        return info


neo4j_client = Neo4jClient()


if __name__ == '__main__':
    # example
    # ret = neo4j_client.example('梅飞')
    # print(ret)
    # ret = neo4j_client.get_ent_graph_g('镇江市广播电视服务公司修理部', 3, 'GS')
    # ret = neo4j_client.get_ent_actual_controller('晟睿电气科技（江苏）有限公司', '')
    # print(ret)

    import time
    from parse.parse_graph import parse
    start = time.time()
    # 1
    # command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {UNISCID: '91321182339145778P'}) return properties(n) as snode, labels(n) as snode_type, r as links"
    command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {NAME: '晟睿电气科技（江苏）有限公司'}) foreach(n in nodes(p) | set n.label=labels(n)[0])  return distinct [n in nodes(p) | properties(n)] as n, [r in filter( link in relationships(p) where toFloat(link.RATE) > 0) | properties(r)] as r"

    # 2
    # command = "match p = (n) -[r:BEE | SPE* .. 3]- (m:GS {NAME: '镇江市广播电视服务公司经营部'}) where n:GS or n:GR return n.ID as ID, n.NAME as NAME, n.NAME_GLLZD as NAME_GLLZD, labels(n) as labels, properties(m) as ATTRIBUTEMAP, r as links"
    # command = "match p = (n) -[r:BEE | SPE* .. 3]- (m:GS {NAME: '镇江市广播电视服务公司经营部'}) where n:GS or n:GR return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links"

    # 3
    # command = "match p = (n:GS {NAME: '镇江新区鸿业精密机械厂'}) -[r:BEE | IPEE | SPE* .. 6]- (m:GS {NAME: '镇江润豪建筑劳务有限公司'}) where n:GS or n:GR return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links"

    rs = neo4j_client.graph.run(command)
    info = rs.data()
    # print('tmp', time.time() - start)
    # print(info)


    ret = parse.get_ent_actual_controller(info)
    # ret = parse.parse(info)
    print(ret)
    # print('end:', time.time() - start)

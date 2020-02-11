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
        # todo 统一社会信用代码查询
        if usccode:
            command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {UNISCID: '%s'}) return properties(n) as snode, labels(n) as snode_type, r as links"
            print(command % usccode)
            rs = self.graph.run(command % usccode)
        else:
            command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {NAME: '%s'}) return properties(n) as snode, labels(n) as snode_type, r as links"
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
    from utils.parse_graph import parse
    start = time.time()
    # 1
    # command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {UNISCID: '91321182339145778P'}) return properties(n) as snode, labels(n) as snode_type, r as links"
    # command = "match p = (n) -[r:IPEE* .. 10]-> (m:GS {NAME: '晟睿电气科技（江苏）有限公司'}) return properties(n) as snode, labels(n) as snode_type, r as links"

    # 2
    # command = "match p = (n) -[r:BEE | SPE* .. 3]- (m:GS {NAME: '镇江市广播电视服务公司经营部'}) where n:GS or n:GR return n.ID as ID, n.NAME as NAME, n.NAME_GLLZD as NAME_GLLZD, labels(n) as labels, properties(m) as ATTRIBUTEMAP, r as links"
    # command = "match p = (n) -[r:BEE | SPE* .. 3]- (m:GS {NAME: '镇江市广播电视服务公司经营部'}) where n:GS or n:GR return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links"

    # 3
    command = "match p = (n:GS {NAME: '镇江新区鸿业精密机械厂'}) -[r:BEE | IPEE | SPE* .. 6]- (m:GS {NAME: '镇江润豪建筑劳务有限公司'}) where n:GS or n:GR return properties(n) as snode, labels(n) as snode_type, properties(m) as enode, labels(m) as enode_type, r as links"

    rs = neo4j_client.graph.run(command)
    info = rs.data()
    print('tmp', time.time() - start)
    # ret = parse.get_ent_actual_controller(info)
    ret = parse.parse(info)
    print(ret)
    print('end:', time.time() - start)

# match (a)-[r:pos]->(x:ent {ENTNAME:'镇江市京沪运输有限公司'})<-[e:inv]-(b) return a,r,x,e,b
# match p = (a) -[]-> (b:ent {ENTNAME: '江苏富联通讯技术有限公司'}) <-[]-(c)  return p

'''
R101	企业对外投资     match p = (b:ent)<-[r2* .. 2]-(n:inv {INVTYPE: '%s'})-[r* .. 2]-(m:ent  {ENTNAME: '晟睿电气科技（江苏）有限公司'}) return p
R102	企业股东         match p=(e:inv) -[r:inv]-> (n:ent {ENTNAME: '%s'}) return p
R103	自然人对外投资   match p = (b:ent)<-[r2* .. 2]-(n:inv {INVTYPE: '%s'})-[r* .. 2]-(m:ent  {ENTNAME: '江苏金国电子有限公司'}) return p
R104	自然人股东       match p=(e:inv) -[r:inv]-> (n:ent {ENTNAME: '%s'}) return p
R105	管理人员公司任职          x
R106	公司管理人员      match p=(n:pos) -[r:pos]-> (e:ent {ENTNAME: '%s'}) return p
R107	分支机构          match p=(n:ent {ENTNAME: '%s'}) -[r:bra]-> (e) return p
R108	总部                     X
R109	企业关联中标              x
R110	中标关联企业              x
R111	企业关联注册地            x
R112	注册地关联企业            x
R113	企业关联邮箱/电话         x
R114	邮箱/电话关联企业         x
R115	企业关联专利              x
R116	专利关联企业              x
R117	企业关联诉讼              x
R118	诉讼关联企业              x
R119	人员关联专利              x
R120	专利关联人员              x
R139	历史企业股东              x
R140	历史企业对外投资          x
R141	历史自然人股东            x
R142	历史自然人对外投资        x
R143	历史公司管理人员          x
R144	历史管理人员公司任职       x
'''

'''
基础语句
    match p = (n)-[r* .. 3]-(m:ent  {ENTNAME: '镇江市广播电视服务公司'}) return p

构造语句
    match p = (n)-[r* .. 3]-(m:ent  {ENTNAME: '镇江市广播电视服务公司'}) return p
    
    level  层级数量
    

企业关系
    企业股东
        match p=(n:inv) -[r*1 .. 3]-> (e:ent {ENTNAME: '%s'}) return p
    企业对外投资
        
    分支机构
    历史企业股东
    历史企业对外投资
    
人员关系
    自然人股东
    自然人对外投资
    公司管理人员
    管理人员其他公司任职
    历史自然人股东                    x
    历史自然人对外投资               x
    历史公司管理人员                x
    历史管理人员其他公司任职        x
    
商业关系        x

潜在关系        x

'''
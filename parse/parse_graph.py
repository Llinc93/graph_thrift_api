import copy, time
from collections import defaultdict


class Parse():

    NODE_TYPE = ['GS']
    LINK_TYPE = ['IPEE', 'SPE', 'LEE', 'IHPEEN', 'SHPEN']

    MAX_LINK = 10
    MAX_NODE = 8

    RELATION_MAP = {
        'IPEE': '投资',
        'SPE': '任职',
        'BEE': '分支机构',
        'WEB': '招投标',
        'RED': '相同办公地',
        'LEE': '共同联系方式',
        'OPEP': '共有专利',
        'LEL': '诉讼',
        'IHPEEN': '历史投资',
        'SHPEN': '历史任职',
    }

    # d表示方向, 1:in 2:out 3:in and out
    CONDITION_MAP = {
        'R101': {'n': 'GS', 'r': 'IPEE', 'd': 2},   # 企业对外投资
        'R102': {'n': 'GS', 'r': 'IPEE', 'd': 1},   # 企业股东
        'R103': {'n': 'GR', 'r': 'IPEE', 'd': 2},   # 自然人对外投资
        'R104': {'n': 'GR', 'r': 'IPEE', 'd': 1},   # 自然人股东
        'R105': {'n': 'GR', 'r': 'SPE', 'd': 2},    # 管理人员其他公司任职
        'R106': {'n': 'GR', 'r': 'SPE', 'd': 1},    # 公司管理人员
        'R107': {'n': 'GR', 'r': 'BEE', 'd': 3},    # 分支机构
        'R108': {'n': 'GR', 'r': 'BEE', 'd': 3},    # 总部
        'R109': {'n': 'GB', 'r': 'WEB', 'd': 3},    # 企业关联中标
        'R110': {'n': 'GB', 'r': 'WEB', 'd': 3},    # 中标关联企业
        'R111': {'n': 'DD', 'r': 'RED', 'd': 3},    # 企业关联注册地
        'R112': {'n': 'DD', 'r': 'RED', 'd': 3},    # 注册地关联企业
        'R113': {'n': ['EE', 'TT'], 'r': 'LEE', 'd': 3},     # 企业关联邮箱 / 电话
        'R114': {'n': ['EE', 'TT'], 'r': 'LEE', 'd': 3},     # 邮箱 / 电话关联企业
        'R115': {'n': 'PP', 'r': 'OPEP', 'd': 3},     # 企业关联专利
        'R116': {'n': 'PP', 'r': 'OPEP', 'd': 3},     # 专利关联企业
        'R117': {'n': 'LL', 'r': 'LEL', 'd': 3},     # 企业关联诉讼
        'R118': {'n': 'LL', 'r': 'LEL', 'd': 3},     # 诉讼关联企业
        # 'R119': {'n': '', 'r': '', 'd': 3},     # 人员关联专利
        # 'R120': {'n': '', 'r': '', 'd': 3},     # 专利关联人员
        # 'R139': {'n': '', 'r': '', 'd': 3},     # 历史企业股东
        # 'R140': {'n': '', 'r': '', 'd': 3},     # 历史企业对外投资
        # 'R141': {'n': '', 'r': '', 'd': 3},     # 历史自然人股东
        # 'R142': {'n': '', 'r': '', 'd': 3},     # 历史自然人对外投资
        # 'R143': {'n': '', 'r': '', 'd': 3},     # 历史公司管理人员
        # 'R144': {'n': '', 'r': '', 'd': 3},     # 历史管理人员其他公司任职
    }
    filter_map = {
        'R101': 'R102',  # 企业对外投资
        'R102': 'R101',  # 企业股东
        'R103': 'R104',  # 自然人对外投资
        'R104': 'R103',  # 自然人股东
        'R105': 'R106',  # 管理人员其他公司任职
        'R106': 'R105',  # 公司管理人员
        'R107': 'R108',  # 分支机构
        'R108': 'R107',  # 总部
        'R109': 'R110',  # 企业关联中标
        'R110': 'R109',  # 中标关联企业
        'R111': 'R112',  # 企业关联注册地
        'R112': 'R111',  # 注册地关联企业
        'R113': 'R114',  # 企业关联邮箱 / 电话
        'R114': 'R113',  # 邮箱 / 电话关联企业
        'R115': 'R116',  # 企业关联专利
        'R116': 'R115',  # 专利关联企业
        'R117': 'R118',  # 企业关联诉讼
        'R118': 'R117',  # 诉讼关联企业
        # 'R119': {'n': '', 'r': '', 'd': 3},     # 人员关联专利
        # 'R120': {'n': '', 'r': '', 'd': 3},     # 专利关联人员
        # 'R139': {'n': '', 'r': '', 'd': 3},     # 历史企业股东
        # 'R140': {'n': '', 'r': '', 'd': 3},     # 历史企业对外投资
        # 'R141': {'n': '', 'r': '', 'd': 3},     # 历史自然人股东
        # 'R142': {'n': '', 'r': '', 'd': 3},     # 历史自然人对外投资
        # 'R143': {'n': '', 'r': '', 'd': 3},     # 历史公司管理人员
        # 'R144': {'n': '', 'r': '', 'd': 3},     # 历史管理人员其他公司任职
    }

    def get_term_v3(self, attIds):
        '''
        根据传入的条件获取节点类型和关系类型，并获取过滤条件
        1. 传入条件过滤
        2. 判断方向
        3. 获取过滤条件
            1. 同一方向的单向, 均为1，或均为2
            2. 不同方向的单向， 1和2
            3. 全部不定向， 均为3
            4. 单向、不定向混合， 1和3， 2和3
        :param attIds:
        :return:
        '''
        filter = []
        # step1 传入条件过滤
        # if 'R103' in attIds and 'R104' not in attIds:
        #     attIds.remove('R103')
        #     filter.append('R104')

        # if 'R105' in attIds and 'R106' not in attIds:
        #     attIds.remove('R105')
        #     filter.append('R106')

        # if 'R142' in attIds and 'R141' not in attIds:
        #     attIds.remove('R142')
        #     filter.append('R141')

        # if 'R144' in attIds and 'R143' not in attIds:
        #     attIds.remove('R144')
        #     filter.append('R143')

        # step2 判断方向，并获取节点和关系
        d = {}
        nodes = set()
        links = set()
        for attId in attIds:
            n = self.CONDITION_MAP[attId]['n']
            r = self.CONDITION_MAP[attId]['r']

            if isinstance(n, list):
                nodes = nodes.union(set(n))
                label = f'{"_".join(n)}_{r}'
            else:
                nodes.add(n)
                label = f'{n}_{r}'
            links.add(r)

            if label in d:
                d[label]['value'].add(self.CONDITION_MAP[attId]['d'])
            else:
                d[label] = {'value': set([self.CONDITION_MAP[attId]['d']]), 'attid': attId}

        # step3 根据方向，获取过滤条件
        direct = ''
        key_action = []
        value_action = []
        for key, value in d.items():
            key_action.append(key)
            value_action.append(sum(value['value']))

        # 1. 同一方向的单向, 均为1，或均为2, 不需要过滤
        if value_action.count(1) == len(value_action):
            direct = 'in'
        elif value_action.count(2) == len(value_action):
            direct = 'out'
        # 2. 全部不定向， 均为3， 不需要过滤
        elif value_action.count(3) == len(value_action):
            direct = 'full'
        # 3. 不同方向的单向, 1和2, 没有不定向, A1和B3;单向、不定向混合， 1和3， 2和3
        else:
            direct = 'full'
            for index in range(len(value_action)):
                if value_action[index] != 3:
                    filter.append(self.filter_map[d[key_action[index]]['attid']])

        return nodes, links, filter, direct

    def get_ent_actual_controller(self, graph, min_rate):
        '''
        根据neo4j的结果，计算受益所有人
        :param graph:
        :return:
        '''
        res_nodes = {}
        res_links = []
        links_set = set()
        end_node_indegree = defaultdict(int)
        for path in graph:
            nodes = path['n']
            links = path['r']
            if not links:
                continue

            # 获取每一条路径上的节点，并计算每一条路径上的综合占比
            tmp_links = {}
            number = 1
            count = 0
            for index in range(len(links)):
                if nodes[index]['ID'] == 'null' or nodes[index+1]['ID'] == 'null':
                    continue

                # 分支关系，比例为1
                if links[index]['label'] == 'BEE':
                    links[index]['RATE'] = 1

                if links[index]['ID'] not in tmp_links.keys():
                    tmp_links[links[index]['ID']] = {'id': nodes[index + 1]['ID'], 'pid': nodes[index]['ID'], 'number': links[index]['RATE'], 'type': links[index]['label']}
                if nodes[index + 1]['ID'] not in res_nodes.keys():
                    res_nodes[nodes[index + 1]['ID']] = {'id': nodes[index + 1]['ID'], 'name': nodes[index + 1]['NAME'], 'number': 0, 'lastnode': 0, 'type': nodes[index + 1]['label'], 'attr': 2, 'path': []}
                end_node_indegree[nodes[index + 1]['ID']] += 1
                if links[index]['RATE'] != 'null':
                    number *= float(links[index]['RATE'])
                else:
                    count += 1

            # 若整条路径的占比都为空，则这条路径的占比为空
            if count == len(links):
                number = 0

            # 计算最终的综合占比
            if nodes[0]['ID'] not in res_nodes.keys():
                res_nodes[nodes[0]['ID']] = {'id': nodes[0]['ID'], 'name': nodes[0]['NAME'], 'number': number, 'lastnode': 0, 'type': nodes[0]['label'], 'attr': 1, 'path': [tmp_links], 'layer': len(links)}
            else:
                if res_nodes[nodes[0]['ID']]['attr'] == 1:
                    res_nodes[nodes[0]['ID']]['number'] += number
                    res_nodes[nodes[0]['ID']]['path'].append(tmp_links)
                else:
                    res_nodes[nodes[0]['ID']] = {'id': nodes[0]['ID'], 'name': nodes[0]['NAME'], 'number': 0, 'lastnode': 0, 'type': nodes[0]['label'], 'attr': 2, 'path': []}

            if nodes[len(links)]['ID'] not in res_nodes.keys():
                res_nodes[nodes[len(links)]['ID']] = {'id': nodes[len(links)]['ID'], 'name': nodes[len(links)]['NAME'], 'number': 0, 'lastnode': 0, 'type': nodes[len(links)]['label'], 'attr': 1, 'path': []}

        # 去除综合占比小于最小投资比例的节点并计算实际控制人
        actions = copy.deepcopy(res_nodes)
        for key, value in actions.items():
            if value['attr'] == 1 and value['number'] < min_rate and value['type'] == 'GR':
                res_nodes.pop(key)
                continue

            if value['attr'] == 1 and value['type'] == 'GR' and value['number'] >= 0.25:
                res_nodes[key]['lastnode'] = 1

            # 将关系加入关系列表
            for i in res_nodes[key].pop('path'):
                for item in i.values():
                    r = (item['id'], item['pid'], item['number'], item['type'])
                    if r not in links_set:
                        res_links.append(item)
                        links_set.add(r)

        # 寻找第10层企业
        for key, value in res_nodes.items():
            if value['type'] == 'GS' and value['attr'] == 1 and value['layer'] == 10:
                res_nodes[key]['lastnode'] = 1

        data = []
        for i in res_nodes.values():
            if i.get('layer'):
                i.pop('layer')
            data.append(i)
        return data, res_links

    def R101(self, link, current, next):
        '''
        过滤企业对外投资
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['id'] and next['ID'] == link['pid'] and link['label'] == 'IPEE' and current['label'] == 'GS' and next['label'] == 'GS':
            return False
        return True

    def R102(self, link, current, next):
        '''
        过滤企业股东
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['pid'] and next['ID'] == link['id'] and link['label'] == 'IPEE' and current['label'] == 'GS' and next['label'] == 'GS':
            return False
        return True

    def R103(self, link, current, next):
        '''
        过滤自然人对外投资
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['id'] and next['ID'] == link['pid'] and link['label'] == 'IPEE' and current['label'] == 'GS' and next['label'] == 'GR':
            return False
        return True

    def R104(self, link, current, next):
        '''
        过滤自然人股东
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['pid'] and next['ID'] == link['id'] and link['label'] == 'IPEE' and current['label'] == 'GR' and next['label'] == 'GS':
            return False
        return True

    def R105(self, link, current, next):
        '''
        过滤管理人员其他公司任职
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['id'] and next['ID'] == link['pid'] and link['label'] == 'SPE' and current['label'] == 'GS' and next['label'] == 'GR':
            return False
        return True

    def R106(self, link, current, next):
        '''
        过滤公司管理人员
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['pid'] and next['ID'] == link['id'] and link['label'] == 'SPE' and current['label'] == 'GR' and next['label'] == 'GS':
            return False
        return True

    def filter_graph(self, path, filter, level, entname):
        '''
        对图数据库返回的结果进行过滤
            1. 去除超过指定层级部分的路径
            2. 按照过滤条件进行过滤
                1. 过滤的节点处于路径的起始节点
                2. 过滤的节点处于路径中间
        :param path:
        :param filter:
        :param level:
        :return:
        '''
        nodes = path['n']
        links = path['r']

        # step1 去除超过指定层级部分的路径
        if len(links) > level and nodes[-1]['NAME'] == entname:
            links = links[len(links) - level:]

        # step2 按照过滤条件进行过滤
        tmp_nodes = [nodes[0]]
        tmp_links = []
        for index in range(len(links)):
            link = links[index]
            cur_node = nodes[index]
            next_node = nodes[index + 1]
            # 按照条件进行过滤
            flag = True
            for condition in filter:
                if hasattr(self, condition):
                    if not getattr(self, condition)(link, cur_node, next_node):
                        flag = False
            if flag:
                tmp_nodes.append(next_node)
                tmp_links.append(link)
            else:
                tmp_nodes = [next_node]
                tmp_links = []

        return {'n': tmp_nodes, 'r': tmp_links}

    def get_node_attrib(self, node):
        '''
        构造attrib
        :param node:
        :return:
        '''
        action = {'id': node['ID'], 'name': node['NAME'], 'type': node['type']}
        if node['label'] in ['PP', 'LL', 'DD', 'EE', 'TT', 'GR', 'GB']:
            action['attibuteMap'] = {'extendNumber': node['extendnumber']}
        else:
            action['attibuteMap'] = {
                'extendNumber': node['extendnumber'],
                'industry_class': node['INDUSTRY'],
                'business_age': node['ESDATE'][:4],
                'province': node['PROVINCE'],
                'registered_capital': node['REGCAP'],
                'regcapcur': node['RECCAPCUR'],
                'business_status': node['ENTSTATUS'],
            }
        return action

    def get_link_attrib(self, link):
        action = {'id': link['ID'], 'name': self.RELATION_MAP[link['type']], 'from': link['pid'], 'to': link['id'], 'type': link['label']}
        if link['label'] == 'IPEE':
            action['attibuteMap'] = {
                'conratio': link['RATE'],
                'holding_mode': link[''],
            }
        elif link['label'] == 'SPE':
            action['attibuteMap'] = {'position': link['POSITION']}
        elif link['label'] == 'LEE':
            action['attibuteMap'] = {'domain'}
        elif link['label'] == 'IHPEEN':
            pass
        elif link['label'] == 'SHPEN':
            pass
        else:
            action['attibuteMap'] = {}
        return action

    def parse_v3(self, graph, filter, level, entname):
        '''
        解析neo4j返回的结果
        :param graph:
        :return:
        '''
        nodes = []
        links = []
        nodes_set = set()
        links_set = set()
        for path in graph:
            if filter:
                path = self.filter_graph(path, filter, level, entname)
            for node in path['n']:
                if node['ID'] not in nodes_set:
                    node = self.get_node_attrib(node)
                    nodes.append(node)
                    nodes_set.add(node['ID'])
            for link in path['r']:
                if link['ID'] not in links_set:
                    link = self.get_link_attrib(link)
                    links.append(link)
                    links_set.add(link['ID'])
        return nodes, links


parse = Parse()

if __name__ == '__main__':
    CONDITION_MAP = {
        'R101': {'n': 'GS', 'r': 'IPEE', 'd': 2},   # 企业对外投资
        'R102': {'n': 'GS', 'r': 'IPEE', 'd': 1},   # 企业股东
        'R103': {'n': 'GR', 'r': 'IPEE', 'd': 2},   # 自然人对外投资
        'R104': {'n': 'GR', 'r': 'IPEE', 'd': 1},   # 自然人股东
        'R105': {'n': 'GR', 'r': 'SPE', 'd': 2},    # 管理人员其他公司任职
        'R106': {'n': 'GR', 'r': 'SPE', 'd': 1},    # 公司管理人员
        'R107': {'n': 'GR', 'r': 'BEE', 'd': 3},    # 分支机构
        'R108': {'n': 'GR', 'r': 'BEE', 'd': 3},    # 总部
        'R109': {'n': 'GB', 'r': 'WEB', 'd': 3},    # 企业关联中标
        'R110': {'n': 'GB', 'r': 'WEB', 'd': 3},    # 中标关联企业
        'R111': {'n': 'DD', 'r': 'RED', 'd': 3},    # 企业关联注册地
        'R112': {'n': 'DD', 'r': 'RED', 'd': 3},    # 注册地关联企业
        'R113': {'n': ['EE', 'TT'], 'r': 'LEE', 'd': 3},     # 企业关联邮箱 / 电话
        'R114': {'n': ['EE', 'TT'], 'r': 'LEE', 'd': 3},     # 邮箱 / 电话关联企业
        'R115': {'n': 'PP', 'r': 'OPEP', 'd': 3},     # 企业关联专利
        'R116': {'n': 'PP', 'r': 'OPEP', 'd': 3},     # 专利关联企业
        'R117': {'n': 'LL', 'r': 'LEL', 'd': 3},     # 企业关联诉讼
        'R118': {'n': 'LL', 'r': 'LEL', 'd': 3},     # 诉讼关联企业
        # 'R119': {'n': '', 'r': '', 'd': 3},     # 人员关联专利
        # 'R120': {'n': '', 'r': '', 'd': 3},     # 专利关联人员
        # 'R139': {'n': '', 'r': '', 'd': 3},     # 历史企业股东
        # 'R140': {'n': '', 'r': '', 'd': 3},     # 历史企业对外投资
        # 'R141': {'n': '', 'r': '', 'd': 3},     # 历史自然人股东
        # 'R142': {'n': '', 'r': '', 'd': 3},     # 历史自然人对外投资
        # 'R143': {'n': '', 'r': '', 'd': 3},     # 历史公司管理人员
        # 'R144': {'n': '', 'r': '', 'd': 3},     # 历史管理人员其他公司任职
    }
    # 企业族谱接口
    # att = 'R101;R102;R103;R104;R106'
    # level = 2
    # entname = '江苏荣马城市建设有限公司'
    # ret = parse.get_term_v3(att.split(';'))
    # print(ret)
    # nodes, links, filter, direct = parse.get_term_v3(att.split(';'))
    # from model.ent_graph import neo4j_client
    # data, flag = neo4j_client.get_ent_graph_g_v3(entname=entname, level=level, node_type='GS', terms=(nodes, links, direct))
    # if not flag:
    #     print('null')
    # nodes, links = parse.parse_v3(data, filter, level, entname)
    # print(len(nodes), nodes)
    # print()
    # print(len(links), links)

    # 企业关联接口
    att = 'R101;R102;R103;R104;R106'
    level = 4
    entname = '江苏荣马城市建设有限公司;江苏荣马实业有限公司'
    node_type, link_type, filter, direct = parse.get_term_v3(att.split(';'))
    print(node_type, link_type, filter, direct)
    from model.ent_graph import neo4j_client
    from itertools import permutations
    nodes = {}
    links = {}
    entNames = entname.split(';')
    for ent_names in permutations(entNames, 2):
        if ent_names[0] != entNames[0]:
            continue
        data = neo4j_client.get_ents_relevance_seek_graph_g_v3(entnames=ent_names, level=level, terms=(node_type, link_type, direct))

        tmp_nodes, tmp_links = parse.parse_v3(data, filter, level, entname)
        for node in tmp_nodes:
            if node['ID'] not in nodes:
                nodes[node['ID']] = node
        for link in tmp_links:
            if link['ID'] not in links:
                links[link['ID']] = link
    print(len(nodes), [node for node in nodes.values()])
    print()
    print(len(links), [link for link in links.values()])
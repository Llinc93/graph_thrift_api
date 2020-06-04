import copy, time
from itertools import combinations
from collections import defaultdict

from parse.my_thread import MyThreadAPOC
from model.ent_graph import neo4j_client


class Parse():

    NODE_TYPE = ['GS']
    LINK_TYPE = ['IPEE', 'SPE', 'LEE', 'IHPEEN', 'SHPEN']

    MAX_LINK = 10
    MAX_NODE = 8

    RELATION_MAP = {
        'IPEE': '投资',
        'IPEES': '投资',
        'IPEER': '投资',
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
        'R101': {'n': 'GS', 'r': 'IPEES', 'd': 2},   # 企业对外投资
        'R102': {'n': 'GS', 'r': 'IPEES', 'd': 1},   # 企业股东
        'R103': {'n': 'GR', 'r': 'IPEER', 'd': 2},   # 自然人对外投资
        'R104': {'n': 'GR', 'r': 'IPEER', 'd': 1},   # 自然人股东
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

    def get_relationshipFilter(self, attIds):
        '''
        根据attIds确定relationshipFilter

        'R101': 'IPEES>',   # 企业对外投资
        'R102': '<IPEES',   # 企业股东
        'R103': 'IPEER>',   # 自然人对外投资
        'R104': '<IPEER',   # 自然人股东
        'R105': 'SPE>',    # 管理人员其他公司任职
        'R106': '<SPE',    # 公司管理人员
        'R107': 'BEE',    # 分支机构
        'R108': 'BRR',    # 总部
        'R109': 'WEB',    # 企业关联中标
        'R110': 'WEB',    # 中标关联企业
        'R111': 'RED',    # 企业关联注册地
        'R112': 'RED',    # 注册地关联企业
        'R113': 'LEE',     # 企业关联邮箱 / 电话
        'R114': 'LEE',     # 邮箱 / 电话关联企业
        'R115': 'OPEP',     # 企业关联专利
        'R116': 'OPEP',     # 专利关联企业
        'R117': 'LEL',     # 诉讼关联企业
        'R118': 'LEL'     # 诉讼关联企业
        'R119': 'LEL',     # 人员关联专利
        'R120': '',     # 专利关联人员
        'R139': '',     # 历史企业股东
        'R140': '',      # 历史企业对外投资
        'R141': '',     # 历史自然人股东
        'R142': ''},     # 历史自然人对外投资
        'R143': '',     # 历史公司管理人员
        'R144': '',     # 历史管理人员其他公司任职
        :param attIds:
        :return:
        '''
        relationshipFilter = []
        attIds = attIds.split(';')

        # 企业投资
        if 'R101' in attIds and 'R102' in attIds:
            relationshipFilter.append('IPEES')
        elif 'R101' in attIds:
            relationshipFilter.append('IPEES>')
        elif 'R102' in attIds:
            relationshipFilter.append('<IPEES')

        # 自然人投资
        if 'R103' in attIds and 'R104' in attIds:
            relationshipFilter.append('IPEER')
        elif 'R103' in attIds:
            relationshipFilter.append('IPEER>')
        elif 'R104' in attIds:
            relationshipFilter.append('<IPEER')

        # 管理人员
        if 'R105' in attIds and 'R106' in attIds:
            relationshipFilter.append('SPE')
        elif 'R105' in attIds:
            relationshipFilter.append('SPE>')
        elif 'R106' in attIds:
            relationshipFilter.append('<SPE')

        # 分支
        if 'R107' in attIds and 'R108' in attIds:
            relationshipFilter.append('BEE')
        elif 'R107' in attIds:
            relationshipFilter.append('BEE>')
        elif 'R108' in attIds:
            relationshipFilter.append('BEE')

        # 中标
        if 'R109' in attIds and 'R110' in attIds:
            relationshipFilter.append('WEB')
        elif 'R109' in attIds:
            relationshipFilter.append('WEB>')
        elif 'R110' in attIds:
            relationshipFilter.append('<WEB')

        # 办公地
        if 'R111' in attIds and 'R112' in attIds:
            relationshipFilter.append('RED')
        elif 'R111' in attIds:
            relationshipFilter.append('RED>')
        elif 'R112' in attIds:
            relationshipFilter.append('<RED')

        # 相同联系方式
        if 'R113' in attIds and 'R114' in attIds:
            relationshipFilter.append('LEE')
        elif 'R113' in attIds:
            relationshipFilter.append('<LEE')
        elif 'R114' in attIds:
            relationshipFilter.append('LEE>')

        # 专利
        if 'R115' in attIds or 'R116' in attIds:
            relationshipFilter.append('OPEP')
        elif 'R115' in attIds:
            relationshipFilter.append('<OPEP')
        elif 'R116' in attIds:
            relationshipFilter.append('OPEP>')

        # 诉讼
        if 'R117' in attIds or 'R118' in attIds:
            relationshipFilter.append('LEL')
        elif 'R117' in attIds:
            relationshipFilter.append('<LEL')
        elif 'R118' in attIds:
            relationshipFilter.append('LEL>')

        return '|'.join(relationshipFilter)

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
                # if links[index]['label'] == 'BEE':
                #     links[index]['RATE'] = 1

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
                    item['type'] = 'IPEE' if item['type'] in ['IPEES', 'IPEER'] else item['type']
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

    def get_final_beneficiary_name(self, graph, min_rate, lcid):
        '''
        根据neo4j的结果，计算受益所有人
        :param graph:
        :return:
        '''
        pids = defaultdict(set)
        actions = {}
        sub_ids = set()
        graph2 = copy.deepcopy(graph)

        nodes = defaultdict(float)
        links = []
        for path in graph:
            tmp_nodes = path['n']
            tmp_links = path['r']
            tmp_path = []
            tmp_number = 1
            while len(tmp_nodes):
                sub = tmp_nodes.pop()
                link = tmp_links.pop() if tmp_links else sub['ID']
                tmp_path.append(link)
                if tmp_path in links:
                    continue

                if sub['ID'] == lcid:
                    nodes[sub['ID']] = 0
                else:
                    nodes[sub['ID']] += tmp_number
                if link and not isinstance(link, str):
                    if link['label'] == 'BEE':
                        link['RATE'] = 1
                    tmp_number *= float(link['RATE'])

        for path in graph2:
            tmp_nodes = path['n']
            tmp_links = path['r']

            ring_detection = defaultdict(int)
            for node in tmp_nodes:
                ring_detection[node['ID']] += 1
            if list(filter(lambda x: x[1] > 1, ring_detection.items())):
                continue

            while len(tmp_nodes):
                sub = tmp_nodes.pop()
                parent = tmp_nodes[-1] if tmp_nodes else None
                link = tmp_links.pop() if tmp_links else None

                if link and link['label'] == 'BEE':
                    link['RATE'] = 1

                if parent:
                    con = (sub['ID'], parent['ID'])
                    sub_ids.add(sub['ID'])
                else:
                    con = (sub['ID'], None)
                if con in actions:
                    continue

                action = {
                    "number": 0 if sub['ID'] == lcid else nodes[sub['ID']],
                    "number_c": float(link['RATE']) if link else None,
                    "children": None if sub['ID'] == lcid else [],
                    "lastnode": 0 if parent else 1,
                    "name": sub['NAME'],
                    "pid": parent['ID'] if parent else None,
                    "id": sub['ID'],
                    "type": sub['label'],
                    "attr": 2 if sub['ID'] == lcid else 1,
                }
                actions[con] = action
                pids[action['pid']] .add(action['id'])

        top = []
        for sid, pid in actions.keys():
            if pid is None and sid not in sub_ids:
                top.append(actions[(sid, pid)])
            actions[(sid, pid)]['children'] = [actions[i, sid] for i in pids[sid]]

        flag = False
        data = []
        for item in top:
            if item['number'] < min_rate:
                continue

            if item['number'] > 0.25 and item['type'] == 'GR':
                flag = True
            else:
                item['lastnode'] = 0

            data.append(item)

        if not flag:
            for item in data:
                if item['type'] == 'GS':
                    item['lastnode'] = 1
        return data

    def R101(self, link, current, next):
        '''
        过滤企业对外投资
        :param link:
        :param current:
        :param next:
        :return:
        '''
        if current['ID'] == link['id'] and next['ID'] == link['pid'] and link['label'] == 'IPEES' and current['label'] == 'GS' and next['label'] == 'GS':
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
        if current['ID'] == link['pid'] and next['ID'] == link['id'] and link['label'] == 'IPEES' and current['label'] == 'GS' and next['label'] == 'GS':
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
        if current['ID'] == link['id'] and next['ID'] == link['pid'] and link['label'] == 'IPEER' and current['label'] == 'GS' and next['label'] == 'GR':
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
        if current['ID'] == link['pid'] and next['ID'] == link['id'] and link['label'] == 'IPEER' and current['label'] == 'GR' and next['label'] == 'GS':
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

    def get_node_attrib(self, node, extendnumbers={}):
        '''
        构造attrib
        :param node:
        :return:
        '''
        action = {'id': node['ID'], 'name': node['NAME'], 'type': node['label']}
        if node['label'] in ['PP', 'LL', 'DD', 'EE', 'TT', 'GR', 'GB']:
            action['attibuteMap'] = {'extendNumber': len(extendnumbers.get(node['ID'], []))}
        else:
            action['attibuteMap'] = {
                'extendNumber': len(extendnumbers.get(node['ID'], [])),
                'industry_class': node['INDUSTRY'],
                'business_age': node['ESDATE'][:4],
                'province': node['PROVINCE'],
                'registered_capital': node['REGCAP'],
                'regcapcur': node['RECCAPCUR'],
                'business_status': node['ENTSTATUS'],
            }
        return action

    def get_nodeAttrib(self, node):
        '''
        构造attrib
        :param node:
        :return:
        '''
        action = {'id': node['ID'], 'name': node['NAME'], 'type': node['label']}
        if node['label'] in ['PP', 'LL', 'DD', 'EE', 'TT', 'GR', 'GB']:
            action['attributeMap'] = {'extendNumber': node['extendNumber'][0]['value'][0] if node.get('extendNumber') else 0}
        else:
            action['attributeMap'] = {
                'extendNumber': node['extendNumber'][0]['value'][0] if node.get('extendNumber') else 0,
                'industry_class': node['INDUSTRY'],
                'business_age': node['ESDATE'][:4],
                'province': node['PROVINCE'],
                'registered_capital': node['REGCAP'],
                'regcapcur': node['RECCAPCUR'],
                'business_status': node['ENTSTATUS'],
            }
        return action

    def get_link_attrib(self, link):
        action = {'id': link['ID'], 'name': self.RELATION_MAP[link['label']], 'from': link['pid'], 'to': link['id'], 'type': link['label']}
        if link['label'] in ['IPEE', 'IPEER', 'IPEES']:
            action['attibuteMap'] = {
                'conratio': link['RATE'],
                'holding_mode': link['RATE_TYPE'],
                'type': 'IPEE'
            }
        elif link['label'] == 'SPE':
            action['attibuteMap'] = {'position': link['POSITION']}
        elif link['label'] in ['LEET', 'LEEE']:
            action['attibuteMap'] = {'domain': link['DOMAIN']}
        elif link['label'] == 'IHPEEN':
            pass
        elif link['label'] == 'SHPEN':
            pass
        else:
            action['attibuteMap'] = {}
        return action

    def common_relationship_filter(self, nodes, links, extendnumbers={}):
        '''
        过滤单条共有关系过滤
        :param links:
        :return:
        '''
        tmp_nodes = []
        tmp_links = []
        filter = {
            'WEB': [],
            'RED': [],
            'LEE': [],
            'OPEP': [],
            'LEL': [],
        }
        link_dict = {
            'WEB': defaultdict(int),
            'RED': defaultdict(int),
            'LEE': defaultdict(int),
            'OPEP': defaultdict(int),
            'LEL': defaultdict(int),
        }
        link_map = {
            'GB': 'WEB',
            'DD': 'RED',
            'EE': 'LEE',
            'TT': 'LEE',
            'PP': 'OPEP',
            'LL': 'LEL',
        }
        for link in links:
            if link['type'] not in ['WEB', 'RED', 'LEE', 'OPEP', 'LEL']:
                continue
            link_dict[link['type']][link['to']] += 1

        for label, item in link_dict.items():
            for key, value in item.items():
                if value < 2 and extendnumbers.get(key, 0) == 0:
                    filter[label].append(key)

        for node in nodes:
            if node['type'] in ['GB', 'PP', 'DD', 'TT', 'EE', 'LL'] and node['id'] in filter[link_map[node['type']]]:
                continue
            tmp_nodes.append(node)

        for link in links:
            if link['type'] in ['WEB', 'RED', 'LEE', 'OPEP', 'LEL'] and link['to'] in filter[link['type']]:
                continue

            if link['type'] in ['IPEES', 'IPEER']:
                link['type'] = 'IPEE'

            tmp_links.append(link)
        return tmp_nodes, tmp_links

    # def ent_graph_parse(self, graph, level, relationshipFilter):
    #     '''
    #     解析neo4j返回的结果并计算extendNumber
    #     :param graph:
    #     :return:
    #     '''
    #     nodes = []
    #     links = []
    #     nodes_set = set()
    #     links_set = set()
    #
    #     for path in graph:
    #         tmp_links = path['r']
    #         tmp_nodes = path['n']
    #
    #         if len(tmp_links) == level:
    #             tmp_nodes[-1]['extendNumber'] = neo4j_client.get_extendNumber(tmp_nodes[-1], relationshipFilter)
    #
    #         for link in tmp_links:
    #             if link['ID'] not in links_set:
    #                 link = self.get_link_attrib(link)
    #                 links.append(link)
    #                 links_set.add(link['id'])
    #
    #         for node in tmp_nodes:
    #             if node['ID'] not in nodes_set:
    #                 node = self.get_nodeAttrib(node)
    #                 nodes.append(node)
    #                 nodes_set.add(node['id'])
    #     return nodes, links

    def ent_graph_parse(self, graph, level):
        nodes = []
        links = []
        nodes_set = set()
        links_set = set()
        extendnumbers = {}

        for path in graph:
            if len(path['r']) > level and len(path['n']) > 2 and path['n'][0] != path['n'][-1]:
                if path['n'][level]['ID'] in extendnumbers:
                    extendnumbers[path['n'][level]['ID']].add(path['r'][level]['ID'])
                else:
                    extendnumbers[path['n'][level]['ID']] = {path['r'][level]['ID']}

        for path in graph:
            tmp_links = path['r']
            tmp_nodes = path['n']

            if len(tmp_links) > level:
                tmp_links = tmp_links[0: level]
                tmp_nodes = tmp_nodes[0: level]

            for link in tmp_links:
                if link['ID'] not in links_set:
                    link = self.get_link_attrib(link)
                    links.append(link)
                    links_set.add(link['id'])

            for node in tmp_nodes:
                if node['ID'] not in nodes_set:
                    node = self.get_node_attrib(node, extendnumbers)
                    nodes.append(node)
                    nodes_set.add(node['id'])

        nodes, links = self.common_relationship_filter(nodes, links, extendnumbers)
        return nodes, links

    def parallel_query(self, entName, level, relationshipFilter):
        threads = []

        # 序列之间的两两组合(Cn2),查询结果取并集
        entNames = list(set(sorted(entName.split(';'))))
        for i in range(len(entNames)):
            t = MyThreadAPOC(i, entNames, level, relationshipFilter)
            threads.append(t)

        for i in threads:
            i.start()
            i.join()

        # for i in threads:
        #     i.join()

        nodes = []
        links = []
        nodes_set = set()
        links_set = set()
        extendnumber = defaultdict(int)
        for i in threads:
            if not i.result:
                continue

            for path in i.result:
                for node in path['n']:
                    if node['ID'] not in nodes_set:
                        nodes.append(self.get_node_attrib(node))
                        nodes_set.add(node['ID'])

                for link in path['r']:
                    if link['ID'] not in links_set:
                        links.append(self.get_link_attrib(link))
                        extendnumber[link['id']] += 1
                        extendnumber[link['pid']] += 1
                        links_set.add(link['ID'])

        names = [i['id'] for i in nodes]
        ret = neo4j_client.get_extendnumber(names, relationshipFilter)
        extendnumbers = [i['value'][0] for i in ret]
        for node in nodes:
            node['attibuteMap']['extendNumber'] = extendnumbers[names.index(node['id'])] - extendnumber[node['id']]
        return list(nodes), list(links)

    # def ent_relevance_seek_graph(self, entNames, level, relationshipFilter):
    #     threads = []
    #
    #     extendnumber = defaultdict(int)
    #
    #     # 序列之间的两两组合(Cn2),查询结果取并集
    #     # entNames = list(set(sorted(entName.split(';'))))
    #     for i in range(len(entNames)):
    #         t = MyThreadAPOC(i, entNames, level, relationshipFilter)
    #         threads.append(t)
    #
    #     for i in threads:
    #         i.start()
    #
    #     links_dict = dict()
    #     nodes_dict = dict()
    #     for i in threads:
    #         i.join()
    #
    #         if not i.result:
    #             continue
    #
    #         nodes = []
    #         links = []
    #         nodes_set = set()
    #         links_set = set()
    #         for path in i.result:
    #
    #             if len(path['r']) == level:
    #                 if path['n'][-1]['ID'] not in nodes_set:
    #                     path['n'][-1]['extendNumber'] = neo4j_client.get_extendNumber(path['n'][-1], relationshipFilter)
    #
    #             for node in path['n']:
    #                 if node['ID'] not in nodes_set:
    #                     nodes_set.add(node['ID'])
    #                     nodes.append(node)
    #
    #             for link in path['r']:
    #                 if link['ID'] not in links_set:
    #                     links_set.add(link['ID'])
    #                     links.append(link)
    #
    #         for link in links:
    #             if link['ID'] in links_dict:
    #                 links_dict[link['ID']]['count'] += 1
    #             else:
    #                 links_dict[link['ID']] = {
    #                     "count": 1,
    #                     "value": self.get_link_attrib(link)
    #                 }
    #
    #         for node in nodes:
    #             if node['ID'] in nodes_dict:
    #                 nodes_dict[node['ID']]['count'] += 1
    #             else:
    #                 nodes_dict[node['ID']] = {
    #                     "count": 1,
    #                     "value": self.get_nodeAttrib(node)
    #                 }
    #
    #     return [node['value'] for node in nodes_dict.values() if node['count'] > 1], [link['value'] for link in links_dict.values() if link['count'] > 1]

    def ent_relevance_seek_graph(self, graph, level, relationshipFilter):
        '''
        解析neo4j返回的结果
        '''
        nodes = []
        links = []
        nodes_set = set()
        links_set = set()

        for path in graph:
            tmp_nodes = path['n']
            tmp_links = path['r']

            for link in tmp_links:
                if link['ID'] not in links_set:
                    link = self.get_link_attrib(link)
                    links.append(link)
                    links_set.add(link['id'])

            for node in tmp_nodes:
                if node['ID'] not in nodes_set:
                    node = self.get_nodeAttrib(node)
                    nodes.append(node)
                    nodes_set.add(node['id'])

        return nodes, links


parse = Parse()




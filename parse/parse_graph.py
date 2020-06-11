import copy
from collections import defaultdict

import config


class Parse(object):

    @staticmethod
    def get_relationship_filter(attIds):
        """
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
        """
        relationship_filter = []
        attIds = attIds.split(';')

        # 企业投资
        if 'R101' in attIds and 'R102' in attIds:
            relationship_filter.append('IPEES')
        elif 'R101' in attIds:
            relationship_filter.append('IPEES>')
        elif 'R102' in attIds:
            relationship_filter.append('<IPEES')

        # 自然人投资
        if 'R103' in attIds and 'R104' in attIds:
            relationship_filter.append('IPEER')
        elif 'R103' in attIds:
            relationship_filter.append('IPEER>')
        elif 'R104' in attIds:
            relationship_filter.append('<IPEER')

        # 管理人员
        if 'R105' in attIds and 'R106' in attIds:
            relationship_filter.append('SPE')
        elif 'R105' in attIds:
            relationship_filter.append('SPE>')
        elif 'R106' in attIds:
            relationship_filter.append('<SPE')

        # 分支
        if 'R107' in attIds and 'R108' in attIds:
            relationship_filter.append('BEE')
        elif 'R107' in attIds:
            relationship_filter.append('BEE>')
        elif 'R108' in attIds:
            relationship_filter.append('BEE')

        # 中标
        if 'R109' in attIds and 'R110' in attIds:
            relationship_filter.append('WEB')
        elif 'R109' in attIds:
            relationship_filter.append('WEB>')
        elif 'R110' in attIds:
            relationship_filter.append('<WEB')

        # 办公地
        if 'R111' in attIds and 'R112' in attIds:
            relationship_filter.append('RED')
        elif 'R111' in attIds:
            relationship_filter.append('RED>')
        elif 'R112' in attIds:
            relationship_filter.append('<RED')

        # 相同联系方式
        if 'R113' in attIds and 'R114' in attIds:
            relationship_filter.append('LEE')
        elif 'R113' in attIds:
            relationship_filter.append('<LEE')
        elif 'R114' in attIds:
            relationship_filter.append('LEE>')

        # 专利
        if 'R115' in attIds or 'R116' in attIds:
            relationship_filter.append('OPEP')
        elif 'R115' in attIds:
            relationship_filter.append('<OPEP')
        elif 'R116' in attIds:
            relationship_filter.append('OPEP>')

        # 诉讼
        if 'R117' in attIds or 'R118' in attIds:
            relationship_filter.append('LEL')
        elif 'R117' in attIds:
            relationship_filter.append('<LEL')
        elif 'R118' in attIds:
            relationship_filter.append('LEL>')

        return '|'.join(relationship_filter)

    @staticmethod
    def get_ent_actual_controller(graph, min_rate):
        """
        根据neo4j的结果，计算受益所有人
        :param graph:
        :param min_rate:
        :return:
        """
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

                if links[index]['ID'] not in tmp_links.keys():
                    tmp_links[links[index]['ID']] = {
                        'id': nodes[index + 1]['ID'],
                        'pid': nodes[index]['ID'],
                        'number': links[index]['RATE'],
                        'type': links[index]['label']
                    }
                if nodes[index + 1]['ID'] not in res_nodes.keys():
                    res_nodes[nodes[index + 1]['ID']] = {
                        'id': nodes[index + 1]['ID'],
                        'name': nodes[index + 1]['NAME'],
                        'number': 0, 'lastnode': 0,
                        'type': nodes[index + 1]['label'],
                        'attr': 2,
                        'path': []
                    }
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
                res_nodes[nodes[0]['ID']] = {
                    'id': nodes[0]['ID'],
                    'name': nodes[0]['NAME'],
                    'number': number,
                    'lastnode': 0,
                    'type': nodes[0]['label'],
                    'attr': 1,
                    'path': [tmp_links],
                    'layer': len(links)
                }
            else:
                if res_nodes[nodes[0]['ID']]['attr'] == 1:
                    res_nodes[nodes[0]['ID']]['number'] += number
                    res_nodes[nodes[0]['ID']]['path'].append(tmp_links)
                else:
                    res_nodes[nodes[0]['ID']] = {
                        'id': nodes[0]['ID'],
                        'name': nodes[0]['NAME'],
                        'number': 0,
                        'lastnode': 0,
                        'type': nodes[0]['label'],
                        'attr': 2,
                        'path': []
                    }

            if nodes[len(links)]['ID'] not in res_nodes.keys():
                res_nodes[nodes[len(links)]['ID']] = {
                    'id': nodes[len(links)]['ID'],
                    'name': nodes[len(links)]['NAME'],
                    'number': 0,
                    'lastnode': 0,
                    'type': nodes[len(links)]['label'],
                    'attr': 1,
                    'path': []
                }

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

    @staticmethod
    def get_final_beneficiary_name(graph, min_rate, lcid):
        """
        根据neo4j的结果，计算受益所有人
        :param graph:
        :param min_rate:
        :param lcid:
        :return:
        """
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

    @staticmethod
    def get_node_attrib(node, extend_numbers):
        """
        构造attrib
        :param node:
        :param extend_numbers:
        :return:
        """
        action = {'id': node['ID'], 'name': node['NAME'], 'type': node['label']}
        if node['label'] in ['PP', 'LL', 'DD', 'EE', 'TT', 'GR', 'GB']:
            action['attibuteMap'] = {'extendNumber': len(extend_numbers.get(node['ID'], []))}
        else:
            action['attibuteMap'] = {
                'extendNumber': len(extend_numbers.get(node['ID'], [])),
                'industry_class': node['INDUSTRY'],
                'business_age': node['ESDATE'][:4],
                'province': node['PROVINCE'],
                'registered_capital': node['REGCAP'],
                'regcapcur': node['RECCAPCUR'],
                'business_status': node['ENTSTATUS'],
            }
        return action

    @staticmethod
    def get_node_attrib2(node):
        """
        构造attrib
        :param node:
        :return:
        """
        action = {'id': node['ID'], 'name': node['NAME'], 'type': node['label']}
        if node['label'] in ['PP', 'LL', 'DD', 'EE', 'TT', 'GR', 'GB']:
            action['attibuteMap'] = {
                    'extendNumber': node['extendNumber'][0]['value'][0] if node.get('extendNumber') else 0
            }
        else:
            action['attibuteMap'] = {
                'extendNumber': node['extendNumber'][0]['value'][0] if node.get('extendNumber') else 0,
                'industry_class': node['INDUSTRY'],
                'business_age': node['ESDATE'][:4],
                'province': node['PROVINCE'],
                'registered_capital': node['REGCAP'],
                'regcapcur': node['RECCAPCUR'],
                'business_status': node['ENTSTATUS'],
            }
        return action

    @staticmethod
    def get_link_attrib(link):
        action = {
            'id': link['ID'],
            'name': config.RELATION_MAP[link['label']],
            'from': link['pid'],
            'to': link['id'],
            'type': link['label']
        }

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

    @staticmethod
    def common_relationship_filter(nodes, links, extend_numbers):
        """
        过滤单条共有关系过滤

        :param nodes:
        :param links:
        :param extend_numbers:
        :return:
        """
        tmp_nodes = []
        tmp_links = []
        filter_dict = {
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
                if value < 2 and extend_numbers.get(key, 0) == 0:
                    filter_dict[label].append(key)

        for node in nodes:
            if node['type'] in ['GB', 'PP', 'DD', 'TT', 'EE', 'LL'] and \
                    node['id'] in filter_dict[link_map[node['type']]]:
                continue
            tmp_nodes.append(node)

        for link in links:
            if link['type'] in ['WEB', 'RED', 'LEE', 'OPEP', 'LEL'] and link['to'] in filter_dict[link['type']]:
                continue

            if link['type'] in ['IPEES', 'IPEER']:
                link['type'] = 'IPEE'

            tmp_links.append(link)
        return tmp_nodes, tmp_links

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

    def ent_relevance_seek_graph(self, graph):
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
                    node = self.get_node_attrib2(node)
                    nodes.append(node)
                    nodes_set.add(node['id'])

        return nodes, links


parse = Parse()

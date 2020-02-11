import config



maps = {
    'GS': {
        'industry_class': 'INDUSTRY',
        'bussiness_age': 'OPFROM',
        'province': 'PROVINCE',
        'registered_capital': 'REGISTERED_CAPITAL',
        'regcapcur': 'REGCAPCUR',
        'business_status': 'ENTSTATUS',
        'extendnumber': 'EXTENDNUMBER',
    },
    'IPEE': {},
    'SPE': {},
}

class Parse():

    NODE_TYPE = ['GS']
    LINK_TYPE = ['IPEE', 'SPE', 'LEE', 'IHPEEN', 'SHPEN']
    ATTLDS = {
        # 节点、关系、里外侧(里 偶数,外奇数,全 0)
        'R101': ['GS', 'IPEE', 1], # 企业对外投资
        'R102': ['GS', 'IPEE', 2], # 企业股东
        'R103': ['GR', 'IPEE', 1], # 自然人对外投资
        'R104': ['GR', 'IPEE', 2], # 自然人股东
        'R105': ['GR', 'BEE', 1], # 管理人员其他公司任职
        'R106': ['GR', 'SPE', 2], # 公司管理人员
        'R107': ['GS', 'BEE', 1], # 分支机构
        'R108': ['GS', 'BEE', 2], # 总部
        # 'R109' # 企业关联中标
        # 'R110' # 中标关联企业
        # 'R111' # 企业关联注册地
        # 'R112' # 注册地关联企业
        # 'R113' # 企业关联邮箱 / 电话
        # 'R114' # 邮箱 / 电话关联企业
        # 'R115' # 企业关联专利
        # 'R116' # 专利关联企业
        # 'R117' # 企业关联诉讼
        # 'R118' # 诉讼关联企业
        # 'R119' # 人员关联专利
        # 'R120' # 专利关联人员
        # 'R139' # 历史企业股东
        # 'R140' # 历史企业对外投资
        # 'R141' # 历史自然人股东
        # 'R142' # 历史自然人对外投资
        # 'R143' # 历史公司管理人员
        # 'R144' # 历史管理人员其他公司任职
    }
    MAX_LINK = 10
    MAX_NODE = 8

    def get_term(self, attlds):
        direction = set()
        nodes = set()
        links = set()
        for attld in attlds:
            tmp = self.ATTLDS[attld]
            nodes.add(tmp[0])
            links.add(tmp[1])
            direction.add(tmp[2])

        nodes_type = list(nodes) if len(nodes) != self.MAX_NODE else []
        links_type = list(links) if len(links) != self.MAX_LINK else []
        return nodes_type, links_type, sum(direction) % 3

    def get_GS(self, node):
        data = {
            'industry_class': node['INDUSTRY'] if node['INDUSTRY'] != 'null' else '',
            'bussiness_age': node['OPFROM'][0:4] if node['OPFROM'] != 'null' else '',
            'province': node['PROVINCE'] if node['PROVINCE'] != 'null' else '',
            'registered_capital': node['REGCAP'] if node['REGCAP'] != 'null' else '',
            'regcapcur': node['RECCAPCUR'] if node['RECCAPCUR'] != 'null' else '',
            'business_status': node['ENTSTATUS'] if node['ENTSTATUS'] != 'null' else '',
            'extendnumber': 0,
        }
        return data

    def get_IPEE(self, link):
        holding_mode = ''
        conratio = float(link['RATE'])
        if conratio == 'null':
            conratio = ''
        elif conratio == 1:
            holding_mode = '全资'
        elif conratio >= 0.5:
            holding_mode = '绝对控股'
        else:
            holding_mode = '控股'
        return {'holding_mode': holding_mode, 'conratio': conratio}

    def get_SPE(self, link):
        return {'position': link['POSITION'] if link['POSITION'] != 'null' else ''}

    def get_LEE(self):
        pass

    def get_IHPEEN(self):
        pass

    def get_SHPEN(self):
        pass

    def get_node_attribute(self, node_type, node):
        return getattr(self, 'get_{}'.format(node_type))(node) if node_type in self.NODE_TYPE else {'extendnumber': 0}

    def get_link_attribute(self, link_type, link):
        return getattr(self, 'get_{}'.format(link_type))(link) if link_type in self.LINK_TYPE else {}

    def get_ent_actual_controller(self, graph):
        '''
        根据neo4j的结果，计算受益所有人
        :param graph:
        :return:
        '''
        ratio_map = {}
        for path in graph:
            print(path)

            ratio = 1
            for path_link in path['links']:
                link_maps = dict(path_link)
                ratio *= float(link_maps['RATE'])

            if path['snode']['ID'] in ratio_map.keys():
                ratio_map[path['snode']['ID']]['ratio'] += ratio
                ratio_map[path['snode']['ID']]['layer'] = max([ratio_map[path['snode']['ID']]['layer'], len(path['links'])])
            else:
                tmp = {
                    'type': path['snode_type'][0],
                    'attr': path['snode'],
                    'link_attr': dict(path['links'][0]),
                    'layer': len(path['links']),
                    'ratio': ratio,
                }
                ratio_map[path['snode']['ID']] = tmp

        rows_per = []
        rows_ent = []
        sum = 0
        for item in ratio_map.values():
            action = {
                'name': item['attr']['NAME'],
                'number': item['ratio'],
                'type': item['type'],
            }
            if item['type'] == 'GS':
                action['per'] = item['attr']['PERSONNAME']
                rows_ent.append(action)
            else:
                sum += item['ratio']
                rows_per.append(action)

        max_person = max(rows_per, key=lambda x: x['number'])
        max_ent = max(rows_ent, key=lambda x: x['number'])

        if max_person['number'] >= 0.25:
            return max_person
        else:
            max_ent['ent'] = max_ent['name']
            max_ent['name'] = max_ent.pop('per')
            return max_ent

    def parse(self, graph):
        '''
        解析neo4j返回的结果
        :param graph:
        :return:
        '''
        nodes = []
        links = []
        for path in graph:
            print(path)

            start_node = {
                'NAME': path['snode']['NAME'],
                'ID': path['snode']['ID'],
                'ATTRIBUTEMAP': self.get_node_attribute(path['snode_type'][0], path['snode']),
                'TYPE': path['snode_type'][0],
            }

            end_node = {
                'NAME': path['enode']['NAME'],
                'ID': path['enode']['ID'],
                'ATTRIBUTEMAP': self.get_node_attribute(path['enode_type'][0], path['enode']),
                'TYPE': path['snode_type'][0],
            }
            for path_link in path['links']:
                link_maps = dict(path_link)
                link_type = list(path_link.types())[0]
                link = {
                    'NAME': config.RELATIONSHIP_MAP[link_type],
                    'ID': link_maps.pop('ID'),
                    'FROM': start_node['ID'],
                    'TO': end_node['ID'],
                    'ATTRIBUTEMAP': self.get_link_attribute(link_type, link_maps),
                    'TYPE': link_type,
                }
                links.append(link)
            nodes.extend([start_node, end_node])

        return nodes, links


parse = Parse()

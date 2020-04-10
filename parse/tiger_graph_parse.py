import hashlib
from copy import deepcopy
from collections import defaultdict

import config


def ent_actual_controller(data, min_rate):
    nodes = []
    links = []
    special = []
    appear = []
    null = []
    while data['results']:
        item = data['results'].pop()
        tmp_nodes = item['nodes']
        tmp_links = item['links']

        for node in tmp_nodes:

            if not node['attributes']['name']:
                null.append(node['v_id'])
                continue

            if node['v_type'] == 'GR' and node['attributes']['@rate'] < min_rate:
                continue

            if node['v_id'] in appear:
                continue

            appear.append(node['v_id'])
            attr = 1
            lastnode = 0
            if node['v_type'] == 'GR' and node['attributes']['@rate'] >= 0.25:
                lastnode = 1
                special.append(node['v_id'])
            if node['attributes']['@top']:
                lastnode = 1
            if node['v_id'] in special:
                attr = 2

            action = {
                'id': node['v_id'],
                'name': node['attributes']['name'],
                'type': node['v_type'],
                'number': node['attributes']['@rate'],
                'attr': attr,
                'lastnode': lastnode,
            }

            nodes.append(action)

        for link in tmp_links:
            if link['to_id'] in null or link['from_id'] in null:
                continue

            action = {
                'id': link['to_id'],
                'pid': link['from_id'],
                'number': link['attributes']['rate']
            }
            links.append(action)

            if link['to_id'] in special:
                special.append(link['from_id'])
            elif link['from_id'] in special:
                special.append(link['to_id'])

    return nodes, links


def get_final_beneficiary_name(data, min_ratio, entname):
    '''
    {
        "number": 0,
        "number_c": "0.15",
        "children": null,
        "lastnode": 0,
        "name": "宁波市赛伯乐招宝创业投资管理有限公司",
        "pid": "2f961e803631b5b48aed07451fe33601",
        "id": "eac5a21c62286a1b3325d8b1baa1d349",
        "type": "GS",
        "attr": 2
    },

    :param data:
    :param min_ratio:
    :return:
    '''
    nids = defaultdict(int)
    actions = []
    pids = defaultdict(set)
    nodes = {}
    links = {}
    while data['results']:
        path = data['results'].pop()

        for link in path['links']:
            pids[link['to_id']].add(link['from_id'])
            links[(link['from_id'], link['to_id'])] = link
            nids[link['to_id']] += 0
            nids[link['from_id']] += 1

        for node in path['nodes']:
            action = {
                "number": node['attributes']['@rate'],
                "number_c": 0,
                "children": [],
                "lastnode": 1 if node['attributes']['@top'] else 0,
                "name": node['attributes']['name'],
                "pid": None,
                "id": node['v_id'],
                "type": node['v_type'],
                "attr": 1,
            }
            if node['v_id'] in nodes and action['number'] <= nodes[node['v_id']]['number']:
                continue
            nodes[node['v_id']] = action

    for pid, sub_ids in pids.items():
        pnode = nodes[pid]
        for sid in sub_ids:
            link = links[(sid, pid)]
            snode = deepcopy(nodes[sid])
            snode['children'] = nodes[sid]['children']
            if snode['name'] == entname:
                snode['attr'] = 2
                snode['children'] = None
                snode['number'] = 0
            snode['pid'] = pid
            snode['number_c'] = link['attributes']['rate']
            pnode['children'].append(snode)

    flag = True
    for nid, count in nids.items():
        if count == 0:
            if nodes[nid]['number'] >= 0.25 and nodes[nid]['number'] >= min_ratio and nodes[nid]['type'] == 'GR':
                nodes[nid]['lastnode'] = 1
                flag = False

    for nid, count in nids.items():
        if count == 0:
            if nodes[nid]['number'] < min_ratio and nodes[nid]['type'] == 'GR':
                continue
            if not flag and nodes[nid]['type'] == "GS" and nodes[nid]['lastnode'] == 1:
                nodes[nid]['lastnode'] = 0
            actions.append(nodes[nid])

    return actions


def get_final_beneficiary_name_v1(data, min_rate, entname):
    '''
    {
        "number": 0,
        "number_c": "0.15",
        "children": null,
        "lastnode": 0,
        "name": "宁波市赛伯乐招宝创业投资管理有限公司",
        "pid": "2f961e803631b5b48aed07451fe33601",
        "id": "eac5a21c62286a1b3325d8b1baa1d349",
        "type": "GS",
        "attr": 2
    },

    :param data:
    :param min_ratio:
    :return:
    '''
    nodes = {}
    links = {}
    pids = defaultdict(set)
    appear = []
    null = []
    nodes_indegree = defaultdict(int)   # 每个节点的入度，0表示为叶子节点
    while data['results']:
        item = data['results'].pop()
        tmp_nodes = item['nodes']
        tmp_links = item['links']

        for node in tmp_nodes:

            if not node['attributes']['name']:
                null.append(node['v_id'])
                continue

            if node['v_type'] == 'GR' and node['attributes']['@rate'] < min_rate:
                continue

            if node['v_id'] in appear:
                continue

            appear.append(node['v_id'])
            action = {
                'number': node['attributes']['@rate'],
                'children': [],
                'lastnode': 1 if node['attributes']['@top'] else 0,
                'name': node['attributes']['name'],
                'id': node['v_id'],
                'type': node['v_type'],
                'attr': 2 if node['attributes']['name'] == entname else 1,
            }
            nodes[node['v_id']] = action

        for link in tmp_links:
            if link['to_id'] in null or link['from_id'] in null:
                continue
            links[(link['from_id'], link['to_id'])] = link
            pids[link['to_id']].add(link['from_id'])
            nodes_indegree['from_id'] += 1

    flag = False
    actions = []
    top_nodes = filter(lambda x:x[1] == 0, nodes_indegree.items())
    for node in top_nodes:
        if node['number'] < min_rate:
            continue

        if node['type'] == 'GR' and node['number'] >= 0.25:
            node['lastnode'] = 1
            flag = True

        node['number_c'] = None
        node['pid'] = None

        stack = [node['id']]
        tmp = [links[(sid, node['id'])] for sid in pids[node['id']]]
        while tmp:
            link = tmp.pop()

            # 检测环，遇到环跳过
            if link['from_id'] in stack:
                continue

            pnode = nodes[links['to_id']]
            snode = nodes[links['from_id']]

            pnode['children'].append(snode)
            snode['number_c'] = link['rate']
            snode['pid'] = pnode['id']

            if snode['name'] == entname:
                stack = [node['v_id']]
            else:
                tmp.extend([links[(sid, node['id'])] for sid in pids[links['from_id']]])
                stack.append(link['from_id'])

        actions.append(node)

    if flag:
        for node in top_nodes:
            if node['type'] == 'GS':
                node['lastnode'] = 0

    return actions


def get_link(link):
    link_type = link['e_type'].split('REV_')[-1]
    action = {
        'id': link['id'],
        'name': config.LINK_NAME[link_type],
        'from': link['from_id'],
        'to': link['to_id'],
        'type': link_type,
        'attibuteMap': {}
    }

    if link_type in ['IPEES', 'IPEER']:
        action['type'] = 'IPEE'
        action['attibuteMap'] = {
            'conratio': link['attributes']['rate'],
            'holding_mode': link['attributes']['rate_type']
        }
    elif link_type in ['LEEE', 'LEET']:
        action['type'] = 'LEE'
        action['attibuteMap'] = {'domain': link['attributes']['domain']}
    elif link_type == 'SPE':
        action['type'] = 'SPE'
        action['attibuteMap'] = {'position': link['attributes']['position']}
    elif link_type in ['IHPEENS', 'IHPEENR']:
        action['type'] = 'IHPEEN'
        action['attibuteMap'] = {
            'his_data': link['attributes']['his_date'],
            'holding_mode': link['attributes']['holding_mode'],
        }
    elif link_type == 'SHPEN':
        action['type'] = ''
        action['attibuteMap'] = {'hisdate': link['attributes']['hisdate']}

    return action


def get_node(node):
    action = {
        'id': node['v_id'],
        'name': node['attributes']['name'],
        'type': node['v_type'] if node['v_type'] != 'ADDR' else 'DD',
        'attibuteMap': {'extendNumber': node['attributes']['@outdegree'] if node['attributes']['@outdegree'] > 0 else 0},
    }
    if action['type'] == 'GS':
        action['attibuteMap']['industry_class'] = node['attributes']['industry']
        action['attibuteMap']['bussiness_age'] = node['attributes']['esdate']
        action['attibuteMap']['province'] = node['attributes']['province']
        action['attibuteMap']['regcapcur'] = node['attributes']['regcap']
        action['attibuteMap']['registered_captial'] = node['attributes']['reccapcur']
        action['attibuteMap']['business_status'] = node['attributes']['entstatus']
    return action


def ent_graph(data):
    nodes = []
    links = []
    rev_link = {}
    ids = []
    null = []
    appear = defaultdict(int)
    if len(data['results']) == 1:
        return nodes, links

    while data['results']:
        item = data['results'].pop()
        tmp_nodes = item['nodes']
        tmp_links = item['links']

        for node in tmp_nodes:
            if not node['attributes']['name']:
                null.append(node['v_id'])

        for link in tmp_links:
            if link['to_id'] in null or link['from_id'] in null:
                continue

            appear[link['from_id']] += 1
            appear[link['to_id']] += 1

            key = ','.join(sorted([link['from_id'], link['to_id'], link['e_type'].split('REV_')[-1]]))
            if key in rev_link and ('REV_' in rev_link[key]['e_type'] or 'REV_' in link['e_type']) and rev_link[key]['e_type'].split('REV_')[-1] == link['e_type'].split('REV_')[-1]:
                if 'REV_' in rev_link[key]['e_type']:
                    rev_link[key] = link
            else:
                rev_link[key] = link

        for node in tmp_nodes:
            if node['v_id'] in ids or node['v_id'] in null:
                continue
            ids.append(node['v_id'])
            if node['attributes']['@outdegree'] > 0:
                node['attributes']['@outdegree'] -= appear[node['v_id']]
            nodes.append(get_node(node))

    for link in rev_link.values():
        link['id'] = hashlib.md5(','.join([link['to_id'], link['from_id'], link['e_type']]).encode('utf8')).hexdigest()
        links.append(get_link(link))

    return nodes, links


def ent_relevance_seek_graph(data):
    nodes = []
    links = []
    ids = []
    rev_link = {}
    appear = defaultdict(int)
    while data:
        tmp_nodes, tmp_links = data.pop()

        for link in tmp_links:
            link['id'] = hashlib.md5(','.join([link['to_id'], link['from_id'], link['e_type']]).encode('utf8')).hexdigest()
            if link['id'] in ids:
                continue

            ids.append(link['id'])
            appear[link['from_id']] += 1
            appear[link['to_id']] += 1

            key = ','.join(sorted([link['from_id'], link['to_id'], link['e_type'].split('REV_')[-1]]))
            if key in rev_link and ('REV_' in rev_link[key]['e_type'] or 'REV_' in link['e_type']) and rev_link[key][
                'e_type'].split('REV_')[-1] == link['e_type'].split('REV_')[-1]:
                if 'REV_' in rev_link[key]['e_type']:
                    rev_link[key] = link
            else:
                rev_link[key] = link

        for node in tmp_nodes:
            if node['v_id'] not in ids:
                if node['attributes']['@outdegree'] > 0:
                    node['attributes']['@outdegree'] -= appear[node['v_id']]
                nodes.append(get_node(node))
                ids.append(node['v_id'])

    for link in rev_link.values():
        link['id'] = hashlib.md5(
            ','.join([link['to_id'], link['from_id'], link['e_type']]).encode('utf8')).hexdigest()
        links.append(get_link(link))

    return nodes, links


if __name__ == '__main__':
    data = {
        'results': [
            {
                'nodes': [
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}}
                ],
                'links': [],
            },
            {
                'nodes': [
                    {'v_id': 2, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'b'}},
                    {'v_id': 6, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'f'}},
                    {'v_id': 11, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'k'}},
                ],
                'links': [
                    {'from_id': 1, 'to_id': 2, 'e_type': 'REV', 'attributes': {'rate': 1}},    # a<-b
                    {'from_id': 1, 'to_id': 6, 'e_type': 'REV', 'attributes': {'rate': 1}},    # a<-f
                    {'from_id': 1, 'to_id': 11, 'e_type': 'REV', 'attributes': {'rate': 1}},    # a<-k
                ],
            },
            {
                'nodes': [
                    {'v_id': 3, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'c'}},
                    {'v_id': 8, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'h'}},
                    {'v_id': 9, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'i'}},
                    {'v_id': 12, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'l'}},
                ],
                'links': [
                    {'from_id': 2, 'to_id': 3, 'e_type': 'REV', 'attributes': {'rate': 1}},    # b<-c
                    {'from_id': 6, 'to_id': 9, 'e_type': 'REV', 'attributes': {'rate': 1}},    # f<-i
                    {'from_id': 6, 'to_id': 8, 'e_type': 'REV', 'attributes': {'rate': 1}},    # f<-h
                    {'from_id': 11, 'to_id': 12, 'e_type': 'REV', 'attributes': {'rate': 1}},    # k<-l
                ],
            },
            {
                'nodes': [
                    {'v_id': 7, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'g'}},
                    {'v_id': 11, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'k'}},
                    {'v_id': 4, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'd'}},
                ],
                'links': [
                    {'from_id': 3, 'to_id': 4, 'e_type': 'REV', 'attributes': {'rate': 1}},    # c<-d
                    {'from_id': 8, 'to_id': 7, 'e_type': 'REV', 'attributes': {'rate': 1}},    # h<-g
                    {'from_id': 9, 'to_id': 7, 'e_type': 'REV', 'attributes': {'rate': 1}},    # i<-g
                    {'from_id': 12, 'to_id': 11, 'e_type': 'REV', 'attributes': {'rate': 1}},    # l<-k
                ],
            },
            {
                'nodes': [
                    {'v_id': 5, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'e'}},
                    {'v_id': 2, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'b'}},
                ],
                'links': [
                    {'from_id': 4, 'to_id': 5, 'e_type': 'REV', 'attributes': {'rate': 1}},    # d<-e
                    {'from_id': 4, 'to_id': 2, 'e_type': 'REV', 'attributes': {'rate': 1}},    # d<-b
                ],
            },
        ]
    }

    import json
    ret = get_final_beneficiary_name_v1(data=data, min_rate=0, entname='a')
    print(json.dumps(ret))
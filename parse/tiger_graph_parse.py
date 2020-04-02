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
            nids[link['from_id']] += 1
            nids[link['to_id']] += 1

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
                "attr": 1.
            }
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
    print(nid)
    for nid, count in nids.items():
        if count == 1:
            if nodes[nid]['number'] < min_ratio and nodes[nid]['type'] == 'GR':
                continue
            if nodes[nid]['number'] >= 0.25 and nodes[nid]['type'] == 'GR':
                nodes[nid]['lastnode'] = 1
                flag = False

    for nid, count in nids.items():
        if count == 1:
            if not flag and nodes[nid]['type'] == "GS" and nodes[nid]['lastnode'] == 1:
                nodes[nid]['lastnode'] = 0
            actions.append(nodes[nid])

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

            links.append(get_link(link))
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
import hashlib
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

    if link['e_type'] in ['IPEES', 'REV_IPEES', 'IPEER', 'REV_IPEER']:
        action['type'] = 'IPEE'
        action['attibuteMap'] = {
            'conratio': link['attributes']['rate'],
            'holding_mode': link['attributes']['rate_type']
        }
    elif link['e_type'] in ['LEEE', 'LEET']:
        action['type'] = 'LEE'
        action['attibuteMap'] = {'domain': link['attributes']['domain']}
    elif link['e_type'] == 'SPE':
        action['type'] = 'SPE'
        action['attibuteMap'] = {'position': link['attributes']['position']}
    elif link['e_type'] in ['IHPEENS', 'IHPEENR']:
        action['type'] = 'IHPEEN'
        action['attibuteMap'] = {
            'his_data': link['attributes']['his_date'],
            'holding_mode': link['attributes']['holding_mode'],
        }
    elif link['e_type'] == 'SHPEN':
        action['type'] = ''
        action['attibuteMap'] = {'hisdate': link['attributes']['hisdate']}

    return action


def get_node(node):
    action = {
        'id': node['v_id'],
        'name': node['attributes']['name'],
        'type': node['v_type'] if node['v_type'] != 'ADDR' else 'DD',
        'attibuteMap': {'extendNumber': node['attributes']['@outdegree']},
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

            link['id'] = hashlib.md5(','.join([link['to_id'], link['from_id'], link['e_type']]).encode('utf8')).hexdigest()
            links.append(get_link(link))
            appear[link['from_id']] += 1
            appear[link['to_id']] += 1

        for node in tmp_nodes:
            if node['v_id'] in ids or node['v_id'] in null:
                continue
            ids.append(node['v_id'])
            node['attributes']['@outdegree'] -= appear[node['v_id']]
            nodes.append(get_node(node))

    return nodes, links


def ent_relevance_seek_graph(data):
    nodes = []
    links = []
    ids = []
    appear = defaultdict(int)
    while data:
        tmp_nodes, tmp_links = data.pop()

        for link in tmp_links:
            link['id'] = hashlib.md5(','.join([link['to_id'], link['from_id'], link['e_type']]).encode('utf8')).hexdigest()
            if link['id'] not in ids:
                links.append(get_link(link))
                ids.append(link['id'])
                appear[link['from_id']] += 1
                appear[link['to_id']] += 1

        for node in tmp_nodes:
            if node['v_id'] not in ids:
                node['attributes']['@outdegree'] -= appear[node['v_id']]
                nodes.append(get_node(node))
                ids.append(node['v_id'])

    return nodes, links
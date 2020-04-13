from copy import deepcopy
from collections import defaultdict


def task(raw_data, params):
    nodes = {}
    links = []
    pids = defaultdict(set)
    appear = []
    null = []
    start = None
    while raw_data['results']:
        item = raw_data['results'].pop()
        tmp_nodes = item['nodes']
        tmp_links = item['links']

        for node in tmp_nodes:

            if not node['attributes']['name']:
                null.append(node['v_id'])
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
                'attr': 2 if node['attributes']['name'] == params['sname'] else 1,
            }
            nodes[node['v_id']] = action
            if node['attributes']['name'] == params['sname']:
                start = node['v_id']

        for link in tmp_links:
            if link['to_id'] in null or link['from_id'] in null:
                continue
            links.append(link)
            pids[link['from_id']].add(link['to_id'])

    links_index = []
    stack = [start]
    tmp_links = [[start]]
    while stack:
        link = tmp_links.pop()
        tmp = stack.pop()
        if tmp not in pids:
            links_index.append(link)
            continue
        for pid in pids[tmp]:
            if pid in link:
                continue
            action = deepcopy(link)
            stack.append(pid)
            action.append(pid)
            tmp_links.append(action)
    print(links_index)

    return nodes, links, False


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

                ],
                'links': [
                    {'from_id': 1, 'to_id': 2, 'e_type': 'REV', 'attributes': {'rate': 1}},    # a<-b
                ],
            },
            {
                'nodes': [
                    {'v_id': 3, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'c'}},
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                ],
                'links': [
                    {'from_id': 1, 'to_id': 2, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-b
                    {'from_id': 1, 'to_id': 6, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-f
                    {'from_id': 1, 'to_id': 11, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-k
                    {'from_id': 1, 'to_id': 11, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-k
                ],
            },
            {
                'nodes': [
                    {'v_id': 7, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'g'}},
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
                ],
                'links': [
                    {'from_id': 4, 'to_id': 5, 'e_type': 'REV', 'attributes': {'rate': 1}},    # d<-e
                ],
            },
            {
                'nodes': [
                    {'v_id': 5, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'e'}},
                ],
                'links': [
                    {'from_id': 4, 'to_id': 5, 'e_type': 'REV', 'attributes': {'rate': 1}},  # d<-e
                ],
            },
            {
                'nodes': [
                    {'v_id': 3, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'c'}},
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                    {'v_id': 1, 'v_type': 'GS', 'attributes': {'@rate': 0, '@top': False, 'name': 'a'}},  #
                ],
                'links': [
                    {'from_id': 4, 'to_id': 5, 'e_type': 'REV', 'attributes': {'rate': 1}},  # d<-e
                    {'from_id': 4, 'to_id': 2, 'e_type': 'REV', 'attributes': {'rate': 1}},  # d<-b
                    {'from_id': 1, 'to_id': 6, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-f
                    {'from_id': 1, 'to_id': 11, 'e_type': 'REV', 'attributes': {'rate': 1}},  # a<-k
                ],
            },
        ]
    }
    nodes, links, flag = task(raw_data=data)
    print(nodes)
    print(links)
    print()
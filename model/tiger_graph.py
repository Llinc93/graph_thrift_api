import re
import time
import requests
from copy import deepcopy
from itertools import combinations
from threading import Thread
from collections import defaultdict

import config


class MyThread(Thread):
    
    def __init__(self, params):
        super(MyThread, self).__init__()
        self.params = params
        self.ret = None

    def run(self):
        # self.ret = task(self.params)
        # self.ret = task_v2(self.params)
        # self.ret = task_v3(self.params)
        self.ret = task_v4(self.params)
        return None


def get_ent_actual_controller(name=None, uniscid=None):
    '''获取实际控股人'''
    params = {
        'name': name if name else uniscid,
        'flag': True if name else False,
    }
    ret = requests.get(url=config.EntActualController, params=params)
    return ret.json()


def get_ent_graph(name, node_type, level, attIds):
    if node_type != 'GS':
        flag = True
    else:
        if len(name) == 32 and re.findall('[a-z0-9]', name):
            flag = True
        else:
            flag = False

    params = {
        'name': name,
        'node_type': node_type,
        'level': level,
        'flag': flag
    }

    for attid in attIds.split(';'):
        params.update(config.ATTIDS_MAP[attid])

    ret = requests.get(url=config.EntGraphUrl, params=params)
    return ret.json()


def task(params):

    s1 = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl, params=params)
    e1 = time.time()
    print('查询耗时', e1 - s1)
    raw_data = ret.json()
    data_nodes = []
    data_links = []
    nodes = {}
    links = defaultdict(list)
    pids = defaultdict(set)
    appear = {}
    null = {}
    start_node = raw_data['results'][0]['nodes'][0]
    end_node = None
    find_flag = False

    node_count = 0
    link_count = 1

    if raw_data['results'].pop()['@@res_flag']:
        while raw_data['results']:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            tmp_links = item['links']

            node_count += len(tmp_nodes)
            link_count += len(tmp_links)

            for node in tmp_nodes:
                if node['attributes']['name'] == params['ename']:
                    end_node = node
                    find_flag = True
                if find_flag:
                    if not node['attributes']['name']:
                        null[node['v_id']] = 0
                        continue
                    if node['v_id'] in appear:
                        continue
                    appear[node['v_id']] = 0
                    nodes[node['v_id']] = node

            if not find_flag:
                continue

            for link in tmp_links:
                if link['to_id'] in null or link['from_id'] in null:
                    continue
                pids[link['from_id']].add(link['to_id'])
                links[(link['from_id'], link['to_id'])].append(link)
        e2 = time.time()
        print('汇总耗时', e2 - e1)
        links_index = []
        stack = [start_node['v_id']]
        tmp_links = [[start_node['v_id']]]
        while stack:
            link = tmp_links.pop()
            tmp = stack.pop()
            if len(link) > params['level'] + 1:
                continue
            if tmp == end_node['v_id']:
                links_index.append(link)
                continue

            for pid in pids[tmp]:
                if pid in link:
                    continue
                action = deepcopy(link)
                action.append(pid)
                stack.append(pid)
                tmp_links.append(action)
        e3 = time.time()
        print('拼接耗时', e3 - e2)
        for link_index in links_index:
            for index in range(len(link_index) - 1):
                data_links.extend(links[(link_index[index], link_index[index + 1])])
                data_nodes.append(nodes[link_index[index]])
            data_nodes.append(nodes[link_index[-1]])
        print('构造耗时', time.time() - e3)
    return data_nodes, data_links, False

def task_v3(params):

    s1 = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl, params=params)
    e1 = time.time()
    print('查询耗时', e1 - s1)
    raw_data = ret.json()
    data_nodes = []
    data_links = []
    nodes = {}
    links = defaultdict(list)
    pids = defaultdict(set)
    appear = {}
    null = {}
    start_node = raw_data['results'][0]['nodes'][0]
    end_node = None
    find_flag = False

    node_count = 0
    link_count = 1

    if raw_data['results'].pop()['@@res_flag']:

        for link in raw_data['results'].pop()['links']:
            if link['to_id'] in null or link['from_id'] in null:
                continue
            pids[link['from_id']].add(link['to_id'])
            links[(link['from_id'], link['to_id'])].append(link)

        while raw_data['results']:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            tmp_links = item['links']

            node_count += len(tmp_nodes)
            link_count += len(tmp_links)

            for node in tmp_nodes:
                if node['attributes']['name'] == params['ename']:
                    end_node = node
                    find_flag = True
                if find_flag:
                    if not node['attributes']['name']:
                        null[node['v_id']] = 0
                        continue
                    if node['v_id'] in appear:
                        continue
                    appear[node['v_id']] = 0
                    nodes[node['v_id']] = node

            if not find_flag:
                continue

        e2 = time.time()
        print('汇总耗时', e2 - e1)
        links_index = []
        stack = [start_node['v_id']]
        tmp_links = [[start_node['v_id']]]
        while stack:
            link = tmp_links.pop()
            tmp = stack.pop()
            if len(link) > params['level'] + 1:
                continue
            if tmp == end_node['v_id']:
                links_index.append(link)
                continue

            for pid in pids[tmp]:
                if pid in link:
                    continue
                action = deepcopy(link)
                action.append(pid)
                stack.append(pid)
                tmp_links.append(action)
        e3 = time.time()
        print('拼接耗时', e3 - e2)
        for link_index in links_index:
            for index in range(len(link_index) - 1):
                data_links.extend(links[(link_index[index], link_index[index + 1])])
                data_nodes.append(nodes[link_index[index]])
            data_nodes.append(nodes[link_index[-1]])
        print('构造耗时', time.time() - e3)
    return data_nodes, data_links, False

def task_v2(params):

    s1 = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl_v2, params=params)
    e1 = time.time()
    print('查询耗时', e1 - s1)
    raw_data = ret.json()
    data_nodes = []
    data_links = []
    start_node = raw_data['results'][0]['nodes'][0]
    end_node = None
    nodes = {}
    stack = []
    links = {}
    links[start_node['v_id']] = set()
    if raw_data['results'].pop()['@@res_flag']:
        find_flag = False
        while len(raw_data['results']) > 1:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            if not find_flag:
                for node in tmp_nodes:
                    if node['attributes']['name'] == params['ename']:
                        end_node = node
                        find_flag = True
                        for node_id, link in zip(node['attributes']['@previous_id'], node['attributes']['@previous_link']):
                            stack.append([node['v_id'], node_id])
                            links[tuple(sorted([node['v_id'], node_id]))] = link
            else:
                tmp = {node['v_id']: node for node in tmp_nodes}
                nodes.update(tmp)
                tmp_stack = []
                while stack:
                    path = stack.pop()
                    if not tmp.get(path[-1]):
                        continue
                    next_list = tmp[path[-1]]['attributes']['@previous_id']
                    next_link = tmp[path[-1]]['attributes']['@previous_link']
                    for next_id, link in zip(next_list, next_link):
                        if next_id in path:
                            if next_id == end_node['v_id']:
                                tmp_stack.append([end_node['v_id'], next_id])
                        else:
                            action = deepcopy(path)
                            action.append(next_id)
                            tmp_stack.append(action)
                        links[tuple(sorted([path[-1], next_id]))] = link

                stack = tmp_stack

    for path in stack:
        for index in range(len(path) - 1):
            data_nodes.append(nodes[path[index]])
            data_links.append(links[list(sorted([path[index], path[index + 1]]))])
    return data_nodes, data_links, False

def task_v4(params):
    s1 = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl_v2, params=params)
    e1 = time.time()
    raw_data = ret.json()
    print('查询耗时', e1 - s1)

    nodes = {}
    links = defaultdict(list)
    stack = defaultdict(list)
    path_list = []
    item = raw_data['results'].pop()
    start_node = item['Start_node']
    end_node = item['End_node']
    data_nodes = []
    data_links = []
    if raw_data['results'].pop()['@@res_flag']:
        tmp_links = raw_data['results'].pop()
        for link in tmp_links:
            links[tuple(sorted([link['to_id'], link['from_id']]))].append(link)

        for index, item in enumerate(raw_data['results'], 1):
            if index == 1:
                for node in item['nodes']:
                    nodes[node['v_id']] = node
                    for previous in node['attributes']['@previous_id']:
                        path_list.append([previous, node['v_id']])
                        stack[(index, node['v_id'])].append([previous, node['v_id']])
            else:
                tmp = set()
                for node in item['nodes']:
                    for previous in node['attributes']['@previous_id']:
                        links = stack[(index - 1, previous)]
                        tmp.add(previous)
                        for link in links:
                            if node['v_id'] in link:
                                continue
                            link.append(node['v_id'])
                            path_list.append([previous, node['v_id']])
                            stack[(index, node['v_id'])].append(link)
                for key in tmp:
                    stack.pop((index - 1, key))

        print('拼接路径耗时:', time.time() - e1)
        for path in path_list:
            print('path', path)
            if path[-1] != end_node['v_id'] or path[0] != start_node['v_id']:
                continue
            nodes[path[0]].pop('@previous_id')
            nodes[path[0]].pop('@previous_link')
            data_nodes[path[0]] = nodes[path[0]]
            for index in range(1, len(path)):
                nodes[path[index]].pop('@previous_id')
                nodes[path[index]].pop('@previous_link')
                data_nodes[path[index]] = nodes[path[index]]
                for link in links[tuple(sorted([path[index - 1], path[index]]))]:
                    data_links.append(link)

    return data_nodes.values(), data_links

def get_ent_relevance_seek_graph(names, attIds, level):
    params = {
        'sname': '',
        'ename': '',
        'level': level,
    }
    for attid in attIds.split(';'):
        params.update(config.ATTIDS_MAP[attid])

    threads = []
    entnames = combinations(names.split(';'), 2)
    for sname, ename in entnames:
        s = time.time()
        params['sname'] = sname
        params['ename'] = ename
        sname, ename = requests.get(url=config.TEST, params=params).json()['results'][0]['nodes']
        if int(sname['attributes']['@outdegree']) > int(ename['attributes']['@outdegree']):
            params['sname'] = ename['attributes']['name']
            params['ename'] = sname['attributes']['name']
        else:
            params['sname'] = sname['attributes']['name']
            params['ename'] = ename['attributes']['name']
        print('度数比较耗时', time.time() - s)
        t = MyThread(params)
        threads.append(t)
        t.start()

    data = []
    for t in threads:
        t.join()
        nodes, links, flag = t.ret
        if flag:
            raise ValueError('查询TigerGraph，错误')
        data.append((nodes, links))
    return data


if __name__ == '__main__':
    get_ent_relevance_seek_graph('1;2;3', 'R101', 3)
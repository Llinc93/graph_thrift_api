import re
import requests
from itertools import combinations
from threading import Thread

import config


class MyThread(Thread):
    
    def __init__(self, params):
        super(MyThread, self).__init__()
        self.params = params
        self.ret = None

    def run(self):
        self.ret = task(self.params)
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
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl, params=params)
    raw_data = ret.json()
    if raw_data['error']:
        return None, None, True

    nodes = []
    links = []
    null = []
    path = set()
    if raw_data['results'].pop()['@@res_flag']:
        snode = raw_data['results']['nodes'][0]
        while raw_data['results']:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            tmp_links = item['links']

            for node in tmp_nodes:
                if not node['attributes']['name']:
                    null.append(node['v_id'])
                    continue

                if node['attributes']['name'] == params['ename']:
                    path.add(node['v_id'])
                    nodes.append(node)
                elif node['v_id'] in path:
                    nodes.append(node)

            for link in tmp_links:
                if link['to_id'] in null or link['from_id'] in null:
                    continue

                if link['to_id'] in path and link['to_id'] != snode['v_id']:
                    path.add(link['from_id'])
                    links.append(link)
    return nodes, links, False


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
        params['sname'] = sname
        params['ename'] = ename

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
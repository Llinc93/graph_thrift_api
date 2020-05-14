import re
import redis
import time
import json
import hashlib
import requests
from itertools import combinations
from threading import Thread

import config


class RedisClient(object):
    def __init__(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=7, decode_responses=True)
        self.r = redis.Redis(connection_pool=pool)


class MyThread(Thread):

    def __init__(self, params):
        super(MyThread, self).__init__()
        self.params = params
        self.ret = None

    def run(self):
        self.ret = task_test(self.params)
        return None

def task_test(params):
    s = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl, params=params)
    raw_data = ret.json()
    e1 = time.time()
    #print(f'查询时间\t{e1 - s}s')

    if raw_data['error']:
        return [], [], True

    if not raw_data['results'].pop()['@@res_flag']:
        return [], [], True

    appear = {}
    nodes = []
    links = []
    repeat = {}
    while raw_data['results']:
        tmp_nodes = raw_data['results'].pop()['nodes']
        for node in tmp_nodes:
            if node['v_id'] in appear or node['attributes']['name'] == params['ename']:
                related = node['attributes'].pop('@related')
                if node['v_id'] not in repeat:
                    nodes.append(node)
                    repeat[node['v_id']] = {}
                    for sid, link in related.items():
                        appear[sid] = 0
                        links.append(link)
                        link["id"] = hashlib.md5(json.dumps(link, ensure_ascii=False).encode('utf8')).hexdigest()
                        repeat[node['v_id']][link["id"]] = 0
                else:
                    for sid, link in related.items():
                        appear[sid] = 0
                        link["id"] = hashlib.md5(json.dumps(link, ensure_ascii=False).encode('utf8')).hexdigest()
                        if link["id"] not in repeat[node['v_id']]:
                            links.append(link)
                            repeat[node['v_id']][link["id"]] = 0

    return nodes, links, False

def get_ent_actual_controller(name=None, uniscid=None):
    '''获取实际控股人'''
    params = {
        'name': name if name else uniscid,
        'flag': True if name else False,
    }
    ret = requests.get(url=config.EntActualController, params=params)
    return ret.json()

def get_final_beneficiary_name(name=None, uniscid=None):
    '''获取实际控股人'''
    params = {
        'name': name if name else uniscid,
        'flag': True if name else False,
    }
    ret = requests.get(url=config.EntFinalBeneficiaryName, params=params)
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


def get_ent_relevance_seek_graph_v2(names, attIds, level):
    params = {
        'sname': '',
        'ename': '',
        'level': level,
    }
    for attid in attIds.split(';'):
        params.update(config.ATTIDS_MAP[attid])

    redis_client = RedisClient()
    threads = []
    entnames = combinations(names.split(';'), 2)
    for sname, ename in entnames:
        s = time.time()
        params['sname'] = redis_client.r.get(sname)
        params['ename'] = redis_client.r.get(ename)
        sname, ename = requests.get(url=config.EntsDegreeCompare, params=params).json()['results'][0]['nodes']
        if int(sname['attributes']['@outdegree']) > int(ename['attributes']['@outdegree']):
            params['sname'] = ename['v_id']
            params['ename'] = sname['attributes']['name']
        else:
            params['sname'] = sname['v_id']
            params['ename'] = ename['attributes']['name']
        #print('test度数比较耗时', time.time() - s)
        t = MyThread(params)
        threads.append(t)
        t.start()

    data = []
    for t in threads:
        t.join()
        nodes, links, flag = t.ret
        if flag:
            continue
            # raise ValueError('查询TigerGraph，错误')
        data.append((nodes, links))
    return data

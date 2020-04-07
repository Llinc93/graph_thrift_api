import csv
import time
import redis
import functools
from collections import defaultdict


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        start = time.time()
        func(self, *args, **kwargs)
        end = time.time()
        print(f'子图: {func.__doc__}\t耗时: {end - start}')
        return None
    return inner


class SearchSubgraph(object):
    '''
    set_data:
        节点    频率    ID     层级
    '''
    def __init__(self):
        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=2, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        # self.node_id = {}
        self.index_list = []
        self.sub_graph = {}

    @print_cost_time
    def set_node(self, frequency_table, flag=True):
        '''第一步：建立接待你映射表并存储'''
        sub_id = 1
        if flag:
            for name, frequence in sorted(frequency_table.items(), key=lambda x:x[1], reverse=True):
                self.r.hmset(name, {'sub_id': sub_id, 'level': 0})
                self.index_list.append(name)
                sub_id += 1
        else:
            for name, frequency_table in sorted(frequency_table.items(), key=lambda x:x[1], reverse=True):
                self.index_list.append(name)
        return None

    @print_cost_time
    def set_link(self, file_content):
        '''构建关系表'''
        for row in file_content:
            id1 = self.r.hget(row[0], 'sub_id')
            id2 = self.r.hget(row[1], 'sub_id')
            if id1 < id2:
                self.r.sadd(f'link_{row[0]}', row[0], row[1])
            else:
                self.r.sadd(f'link_{row[1]}', row[1], row[0])
        return None

    @print_cost_time
    def run(self, frequency_table, file_content, flag=True):
        '''总共耗时'''
        if flag:
            self.set_node(frequency_table)
            self.set_link(file_content)
        else:
            self.set_node(frequency_table, flag=False)

        for name in self.index_list:
            next_nodes = self.r.smembers(f'link_{name}')
            if not next_nodes:
                print(0)
                continue
            sids = []
            nodes = []
            for node in next_nodes:
                sids.append(self.r.hget(node, 'sub_id'))
                nodes.append(node)
            min_sid = min(sids)

            for node, sid in zip(nodes, sids):
                if sid == min_sid:
                    continue
                level = self.r.hget(node, 'level')
                self.r.hset(node, 'sub_id', min_sid)
                self.r.hset(node, 'level', level + 1)
        return None

    @print_cost_time
    def get_all_data(self):
        '''获取全部数据'''
        keys = self.r.keys()
        for key in keys:
            if key.startswith('link_'):
                continue
            node = self.r.hgetall(key)
            if self.sub_graph.get(node['sub_id']):
                if self.sub_graph[node['sub_id']]['level'] < node['level']:
                    self.sub_graph[node['sub_id']]['level'] = node['level']
                self.sub_graph[node['sub_id']]['count'] += 1
            else:
                self.sub_graph[node['sub_id']] = {'count': 1, 'level': 0}

        count = 0
        for key, value in self.sub_graph.items():
            print(f'子图: {key}\t节点数量: {value["count"]}\t层级: {value["level"]}')
            count += 1
        print(f'共有子图: {count}')
        return None


def get_frequency_table(file):
    frequency_table = defaultdict(int)
    file_content = []
    with open(file, 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            frequency_table[row[0]] += 1
            frequency_table[row[1]] += 1
            file_content.append(row)
    return frequency_table, file_content


if __name__ == '__main__':
    s = time.time()
    file = '/home/20200220csv/tmp/501/target/target_1.csv'
    frequency_table, file_content = get_frequency_table()
    print(f'构建频率表耗时：{time.time() - s}')
    obj = SearchSubgraph()
    obj.run(frequency_table, file_content, flag=True)
    obj.get_all_data()
    print('end')
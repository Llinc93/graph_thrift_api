import csv
import json
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
        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        self.index_list = []
        self.subid_list = []
        self.sub_graph = {}
        self.nodes = set()

    @print_cost_time
    def set_node(self, frequency_table, flag=True):
        '''第一步：建立接待你映射表并存储'''
        f = open('frequency_table.txt', 'w', encoding='utf8')
        sorted_frequency_table = sorted(frequency_table.items(), key=lambda x:x[1], reverse=True)
        sub_id = 1
        if flag:
            for name, frequence in sorted_frequency_table:
                f.write(f'{name}\t{sub_id}\t{frequence}\n')
                self.r.hmset(name, {'sub_id': sub_id, 'level': 0, 'frequency': frequence})
                self.r.sadd(f'subid_{sub_id}', name)
                self.index_list.append(name)
                sub_id += 1
        else:
            for name, frequence in sorted_frequency_table:
                self.index_list.append(name)
                f.write(f'{name}\t{sub_id}\t{frequence}\n')
        f.close()
        with open('index_list.txt', 'w', encoding='utf8') as f:
            f.write(json.dumps(self.index_list))
        return None

    @print_cost_time
    def set_link(self, file_content):
        '''构建关系表'''
        for row in file_content:
            id1 = int(self.r.hget(row[0], 'sub_id'))
            id2 = int(self.r.hget(row[1], 'sub_id'))
            if id1 <= id2:
                self.r.sadd(f'link_{row[0]}', row[0], row[1])
            elif id2 < id1:
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
            if not self.r.exists(f'link_{name}'):
                continue
            next_nodes = self.r.smembers(f'link_{name}')
            # if not next_nodes:
            #     self.tmp_name.append(name)
            #     continue
            sids = []
            nodes = []
            for node in next_nodes:
                self.nodes.add(node)
                sids.append(int(self.r.hget(node, 'sub_id')))
                nodes.append(node)
            min_sid = min(sids)

            for node, sid in zip(nodes, sids):
                level = int(self.r.hget(node, 'level'))
                self.r.hset(node, 'sub_id', min_sid)
                self.r.hset(node, 'level', level + 1)
        return None

    @print_cost_time
    def run2(self, file_content):
        '''run2'''
        for row in file_content:
            id1 = int(self.r.hget(row[0], 'sub_id'))
            id2 = int(self.r.hget(row[1], 'sub_id'))
            if id1 == id2:
                continue
            if id1 < id2:
                nodes = self.r.smembers(id2)
                for node in nodes:
                    self.r.hset(node, 'sub_id', id1)
                    self.r.sadd(f'subid_{id1}', node)
                else:
                    self.r.delete(f'subid_{id2}')
            else:
                nodes = self.r.smembers(id1)
                for node in nodes:
                    self.r.hset(node, 'sub_id', id2)
                    self.r.sadd(f'subid_{id2}', node)
                else:
                    self.r.delete(f'subid_{id1}')

    @print_cost_time
    def get_all_data(self):
        '''获取全部数据'''
        keys = self.r.keys('subid_*')
        count = 0
        for key in keys:
            count += 1
            node = self.r.smembers(key)
            count += 1
            print(f'子图: {key.split("_")[1]}\t节点数量: {len(node)}')
            count += 1
        print(f'共有子图: {len(keys)}\t节点数量: {count}')
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
    file = '/home/20200220csv/target.csv'
    frequency_table, file_content = get_frequency_table(file)
    print(f'构建频率表耗时：{time.time() - s}')
    obj = SearchSubgraph()
    # obj.run(frequency_table, file_content, flag=True)
    obj.run2(file_content)
    obj.get_all_data()
    print('end')
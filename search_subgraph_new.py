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
        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)
        self.node_id = {}
        self.file = []

    @print_cost_time
    def set_node(self, frequency_table):
        '''第一步：建立接待你映射表并存储'''
        sub_id = 1
        for name, frequence in sorted(frequency_table.items(), key=lambda x:x[1], reverse=True):
            self.r.hmset(name, {'sub_id': sub_id, 'level': 0})
            # self.node_id[name] = sub_id
            sub_id += 1
        return None

    @print_cost_time
    def set_link(self, file_content):
        for row in file_content:
            if self.node_id[row[0]] < self.node_id[row[1]]:
                self.r.rpush(row[0], row[1])
                self.file.append((row[0], row[1]))
            else:
                self.r.rpush(row[1], row[0])
                self.file.append((row[1], row[0]))
        return None

    @print_cost_time
    def run(self, frequency_table, file_content):
        '''总共耗时'''
        self.set_node(frequency_table)

        # self.set_link(file_content)

        for row in file_content:
            r1 = self.r.hget(row[0], 'sub_id')
            r2 = self.r.hget(row[1], 'sub_id')

            min_id = min(r1, r2)
            if r1 != min_id:
                self.r.hmset(name=row[0], mapping={'sub_id': min_id, 'level'})


        return

if __name__ == '__main__':
    s = time.time()
    file = '/home/20200220csv/tmp/501/target/target_1.csv'
    frequency_table = defaultdict(int)
    file_content = []
    with open(file, 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            frequency_table[row[0]] += 1
            frequency_table[row[1]] += 1
            file_content.append(row)
    print(f'构建频率表耗时：{time.time() - s}')
    obj = SearchSubgraph()
    obj.run(frequency_table, file_content)
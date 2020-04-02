'''
子图查找：
    1. 查找文件中频率为1的节点 A
    2. A的关联节点频率也为1
    3 确定 1-1 子图
'''

import os
import csv
import time
import functools
import multiprocessing
from collections import defaultdict


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        start = time.time()
        number, graph_index = func(self, *args, **kwargs)
        end = time.time()
        print(f'子图: {number}\t数量: {graph_index}\t耗时: {end - start}')
        return graph_index

    return inner


class SearchSubgraphReverse(object):
    '''
    反向查找不连通子图
    '''

    FILE = '/home/20200220csv/tmp/target_3.csv'
    ROOT = '/home/20200220csv/tmp'
    TMP = '/home/20200220csv/tmp/tmp_subgraph.csv'

    TARGET = '/home/20200220csv/tmp/sub_graph_%s_1.csv'

    NUMBER = 1
    SUM = 2

    def __init__(self):
        self.frequency_table = defaultdict(int)
        self.file_content = []
        self.content_map = defaultdict(list)
        self.filter_indexs = set()
        self.flag = True

    def get_frequency_table(self):
        with open(self.FILE, 'r', encoding='utf8') as f:
            raeder = csv.reader(f)
            index = 0
            for row in raeder:
                for item in row:
                    self.frequency_table[item] += 1
                    self.content_map[item].append(index)
                self.file_content.append(row)
                index += 1
        return None

    @print_cost_time
    def search_graph(self):
        self.get_frequency_table()
        if not list(filter(lambda x:self.frequency_table[x] == 1, self.frequency_table.keys())):
            self.flag = False
            return f'{self.NUMBER}_1', 0
        records = filter(lambda x:self.frequency_table[x] == self.NUMBER, self.frequency_table.keys())
        count = 0
        for record in records:
            indexs = self.content_map[record]
            for index in indexs:
                row = self.file_content[index]
                if self.frequency_table[row[0]] + self.frequency_table[row[1]] == self.NUMBER + 1:
                    self.filter_indexs.add(index)
                    count += 1
        return f'{self.NUMBER}_1', count

    def run(self):
        while self.flag:
            count = self.search_graph()
            if count == 0:
                self.NUMBER += 1
                self.frequency_table = defaultdict(int)
                self.file_content = []
                self.content_map = defaultdict(list)
                self.filter_indexs = set()
                continue
            # todo 生成file文件
            os.system(f'mv {self.FILE} {self.TMP}')
            file = os.path.join(self.ROOT, 'search_subgraph_reverse.csv')
            target_f = open(file, 'w', encoding='utf8')
            writer = csv.writer(target_f)

            sub_graph = open(self.TARGET % self.NUMBER, 'w', encoding='utf8')
            sub_writer = csv.writer(sub_graph)
            for index, row in enumerate(self.file_content):
                if index in self.filter_indexs:
                    sub_writer.writerow(row)
                else:
                    writer.writerow(row)

            target_f.close()
            sub_graph.close()
            self.FILE = file
            self.NUMBER += 1
            self.frequency_table = defaultdict(int)
            self.file_content = []
            self.content_map = defaultdict(list)
            self.filter_indexs = set()


if __name__ == '__main__':
    obj = SearchSubgraphReverse()
    obj.run()
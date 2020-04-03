'''
子图查找：
    1. 查找全文件(B)里频率最高的节点A
    2. 多进程：查找直接与A连接的节点,得到多个关联节点文件和多个非关联节点文件
    3. 分别合并多个进程得到的文件，同时得到关联节点列表a和非关联节点列表b
    4. 用a替代A，用b替代B，重复1、2、3、4，直到a为空，完成子图查找

命名格式：
    TMP文件(第index次遍历文件)： /opt/tmp/target/target_{index}.csv
    TMP子文件(第index次的第n进程遍历文件)： /opt/tmp/target/target_{index}/n.csv

    子图文件(第index次子图文件)： /opt/tmp/graph/graph_{index}.csv
    子图子文件(第index次的第n进程子图子文件)： /opt/tmp/graph/graph_{index}/n.csv

'''

import os
import csv
import time
import shutil
import functools
import multiprocessing
from collections import defaultdict


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        start = time.time()
        graph_index, node_count, frequency = func(self, *args, **kwargs)
        end = time.time()
        print(f'子图: {graph_index}\t节点数量: {node_count}\t第一层频率: {frequency}\t耗时: {end - start}')
        return None
    return inner


class MyProcess(multiprocessing.Process):

    def __init__(self, current, file_content):
        super(MyProcess, self).__init__()
        self.current = current
        self.file_content = file_content
        self.flag = True

    def run(self):
        length = len(self.current)
        filter_index = []
        for index in range(len(self.file_content)):
            row = self.file_content[index]
            flag = False
            for i in row:
                if i in self.current:
                    flag = True
                    break
            if flag:
                for i in row:
                    self.current.add(i)
                filter_index.append(index)
        for index in filter_index:
            self.file_content.pop(index)
        self.flag = True if length != len(self.current) else False
        return None


class SearchSubgraph(object):
    '''查找不连通子图'''

    ROOT = '/home/20200220csv/tmp'
    # TMP = '/home/20200220csv/tmp/1/tmp'
    # if not os.path.exists(TMP):
    #     os.makedirs(TMP)

    TARGET = '/home/20200220csv/tmp/501/target'
    if not os.path.exists(TARGET):
        os.makedirs(TARGET)

    GRAPH = '/home/20200220csv/tmp/501/'
    if not os.path.exists(GRAPH):
        os.makedirs(GRAPH)

    TARGET_CSV = '/home/20200220csv/tmp/501/target/target_1.csv'
    NUMBER = multiprocessing.cpu_count()

    def __init__(self):
        self.current = set()   # 当前迭代的联通节点
        self.fre_current = 0
        self.current_file = self.TARGET_CSV  # 当前迭代的目标文件
        self.tmp_graph_file = None  # 上一次迭代的连通节点
        self.target_dir = None
        self.graph_dir = None
        self.init_flag = True
        self.file_content = []

    def sub_init(self, num):
        '''自循环初始化（获取频率最高的ID）'''
        frequency_table = defaultdict(int)
        with open(self.TARGET_CSV, 'r', encoding='utf8') as f:
            for row in csv.reader(f):
                frequency_table[row[0]] += 1
                frequency_table[row[1]] += 1
                self.file_content.append(row)
        self.current = {max(frequency_table.items(), key=lambda x: x[1])[0]}
        self.fre_current = frequency_table[list(self.current)[0]]

        self.init_flag = False
        self.target_dir = os.path.join(self.TARGET, f'target_{num + 1}.csv')
        return None

    @print_cost_time
    def run(self, graph_index):
        '''运行,查找子图'''

        flag = True
        num = 1

        # 寻找子图(1次)
        while flag:
            if self.init_flag:
                self.sub_init(num)

            ps = []
            interval = len(self.file_content) // self.NUMBER + 1
            for i in range(self.NUMBER):
                ps.append(MyProcess(self.current, self.file_content[i * interval: (i+1) * interval]))

            for p in ps:
                p.start()

            self.file_content = []

            flag = False
            for p in ps:
                p.join()
                self.current |= p.current
                self.file_content.append(p.file_content)
                if p.flag:
                    flag = True

            num += 1

        with open(self.target_dir, 'w', encoding='utf8') as t_f:
            for row in self.file_content:
                t_f.write(f"{','.join(row)}\n")

        node_count = len(self.current)
        with open(f'{self.GRAPH}/graph_nodes.csv', 'w', encoding='utf8') as f:
            for i in self.current:
                f.write(f'{i}\n')

        return graph_index, node_count, self.fre_current, num - 1

    def clean(self, graph_index):
        '''清理上次遗留的文件'''
        if os.path.exists(os.path.join(self.ROOT, graph_index, 'tmp')):
            shutil.rmtree(os.path.join(self.ROOT, graph_index, 'tmp'))
            shutil.rmtree(os.path.join(self.ROOT, graph_index, 'target'))
        return None

    def init(self, index):
        '''循环初始化(设置存储路径)'''
        self.TMP = f'/home/20200220csv/tmp/{index}/tmp'
        if not os.path.exists(self.TMP):
            os.makedirs(self.TMP)

        self.TARGET = f'/home/20200220csv/tmp/{index}/target'
        if not os.path.exists(self.TARGET):
            os.makedirs(self.TARGET)

        self.GRAPH = f'/home/20200220csv/tmp/{index}'
        if not os.path.exists(self.GRAPH):
            os.makedirs(self.GRAPH)

        self.init_flag = True

        self.TARGET_CSV = os.path.join(self.TARGET, "target_1.csv")
        self.current = set()   # 当前迭代的联通节点
        self.target_dir = None
        self.graph_dir = None
        self.init_flag = True

        self.clean(str(index - 1))
        return None

    def main(self):
        '''获取所有的子图'''
        index = 501
        while True:
            self.init(index)
            self.run(index)
            if index > 30000:
                break
            index += 1
        print('找找所有的图完成！')
        return None

if __name__ == '__main__':
    obj = SearchSubgraph()
    obj.main()

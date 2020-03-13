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
import functools
import multiprocessing
from collections import defaultdict


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        start = time.time()
        func(self, *args, **kwargs)
        end = time.time()
        print(dir(func))
        print(f'{func.__doc__}耗时: {end - start}')
        return None
    return inner


class SearchSubgraph(object):
    '''查找不连通子图'''

    FILES = [
        ('/opt/csv/20200220/qiyefenzhi.csv', 'BEE'),
        ('/opt/csv/20200220/qiyetouzi.csv', 'IPEE'),
        ('/opt/csv/20200220/ziranrentouzi.csv', 'IPEE')
    ]
    TMP = '/opt/tmp/tmp'
    TARGET = '/opt/tmp/target'
    GRAPH = '/opt/tmp/graph'
    TARGET_CSV = '/opt/tmp/target/target_1.csv'
    NUMBER = 33

    def __init__(self):
        self.previous = set()  # 前次迭代的连通节点
        self.current = set()   # 当前迭代的联通节点
        self.currnt_file = self.TARGET_CSV

    def IPEE(self, row):
        return [row[0], row[-1]]

    def BEE(self, row):
        return row

    @print_cost_time
    def merge_csv(self):
        '''
        将多个文件合并为目标文件
        :return:
        '''
        for file, label in self.FILES:
            read = open(file, 'r', encoding='utf8')
            write = open(self.TARGET_CSV, 'w', encoding='utf8')
            number = 1
            reader = csv.reader(read)
            writer = csv.writer(write)
            for row in reader:
                if number == 1:
                    continue
                writer.writerow(f'{getattr(self, label)(row)}\n')
        print('文件合并完成！')

    @print_cost_time
    def init(self, num):
        '''获取频率最高的ID'''
        self.previous |= self.current

        frequency_table = defaultdict(int)
        with open(self.currnt_file, 'r', encoding='utf8') as f:
            for row in csv.reader(f):
                frequency_table[row[0]] += 1
                frequency_table[row[1]] += 1
        self.current = {max(frequency_table.items(), key=lambda x: x[1])}

        target_dir = os.path.join(self.TARGET, f'target_{num + 1}')
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        graph_dir = os.path.join(self.GRAPH, f'graph_{num}')
        if not os.path.exists(graph_dir):
            os.makedirs(graph_dir)

        return None

    def task(self, num, pos, file):
        '''
        筛选
        :param num:
        :param pos:
        :param file:
        :return:
        '''
        target_file = os.path.join(self.TARGET, f'target_{num + 1}', f'{pos}.csv')    # 不连通节点文件
        graph_file = os.path.join(self.GRAPH,  f'graph_{num}', f'{pos}.csv')          # 连通节点文件
        with open(file, 'r', encoding='utf8') as f:
            for row in csv.reader(f):

    def merge_sub_file(self):
        '''合并子文件'''
        flag = True
        pass
        return flag

    @print_cost_time
    def run(self):
        # step1 合并文件
        self.merge_csv()

        # step2 迭代
        flag = True
        num = 1

        # 寻找子图(1次)
        while flag:
            p = multiprocessing.Pool(self.NUMBER)

            # 获取筛选条件
            self.init(num)

            # 切分文件
            files = [f'{self.TMP}/{i}.csv' for i in range(self.NUMBER)]
            fs = [open(k, 'w', encoding='utf8') for k in files]

            with open(os.path.join(self.TARGET, f'target_{num}.csv'), 'r', encoding='utf8') as f:
                for index, row in enumerate(csv.reader(f), 2):
                    pos = index % self.NUMBER
                    csv.writer(fs[pos]).writerow(row)
            for f in fs:
                f.close()

            # 每份文件都由子进程进行计算
            for i in range(self.NUMBER):
                p.apply_async(self.task, args=(num, pos, files[pos]))
            p.close()
            p.join()

            # 合并文件
            flag = self.merge_sub_file()

    @print_cost_time
    def test(self, h):
        '''测试'''
        print(h)
        time.sleep(2)
        return None


if __name__ == '__main__':
    SearchSubgraph().test(123)
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


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self):
        start = time.time()
        func(self)
        end = time.time()
        print(dir(func))
        print(f'{func.__doc__}耗时: {end - start}')
        return None
    return inner


class SearchSubgraph(object):
    '''查找不连通子图'''

    FILES = [
        ('/opt/csv/xx.csv', 'IPEE'),
        ('/opt/csv/xx.csv', 'BEE'),
    ]
    TARGET = '/opt/tmp/target'
    GRAPH = '/opt/tmp/graph'
    TARGET_CSV = '/opt/tmp/target/target_1.csv'

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
                writer.write(f'{getattr(self, label)(row)}\n')
        print('文件合并完成！')

    def run(self):
        pass

    @print_cost_time
    def test(self):
        '''测试'''
        time.sleep(2)
        return None


if __name__ == '__main__':
    SearchSubgraph().test()
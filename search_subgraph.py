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
import subprocess
import multiprocessing
from collections import defaultdict


def print_cost_time(func):
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        start = time.time()
        graph_index, node_count, frequency = func(self, *args, **kwargs)
        end = time.time()
        # print(f'{func.__doc__}耗时: {end - start}')
        print(f'子图: {graph_index}\t节点数量: {node_count}\t第一层频率: {frequency}\t耗时: {end - start}')
        return None
    return inner


class SearchSubgraph(object):
    '''查找不连通子图'''

    FILES = [
        ('/home/20200220csv/qiyefenzhi.csv', 'BEE'),
        ('/home/20200220csv/qiyetouzi.csv', 'IPEE'),
        ('/home/20200220csv/ziranrentouzi.csv', 'IPEE')
    ]
    ROOT = '/home/20200220csv/tmp'
    TMP = '/home/20200220csv/tmp/1/tmp'
    if not os.path.exists(TMP):
        os.makedirs(TMP)

    TARGET = '/home/20200220csv/tmp/1/target'
    if not os.path.exists(TARGET):
        os.makedirs(TARGET)

    GRAPH = '/home/20200220csv/tmp/1/graph'
    if not os.path.exists(GRAPH):
        os.makedirs(GRAPH)

    TARGET_CSV = '/home/20200220csv/tmp/1/target/target_1.csv'
    NUMBER = 33

    def __init__(self):
        self.previous = set()  # 前次迭代的连通节点
        self.current = set()   # 当前迭代的联通节点
        self.fre_current = 0
        self.current_file = self.TARGET_CSV  # 当前迭代的目标文件
        self.tmp_graph_file = None  # 上一次迭代的连通节点
        self.target_dir = None
        self.graph_dir = None
        self.init_flag = True
        self.TMP = None
        self.TARGET = None
        self.GRAPH = None

    def IPEE(self, row):
        return [row[0], row[-1]]

    def BEE(self, row):
        return row

    # @print_cost_time
    def merge_csv(self):
        '''将多个文件合并为目标文件'''
        write = open(self.TARGET_CSV, 'w', encoding='utf8')
        writer = csv.writer(write)
        for file, label in self.FILES:
            read = open(file, 'r', encoding='utf8')
            number = 1
            reader = csv.reader(read)
            for row in reader:
                if number == 1:
                    number += 1
                    continue
                ret = getattr(self, label)(row)
                writer.writerow(ret)
            read.close()
        write.close()
        # print('文件合并完成！')
        return None

    # @print_cost_time
    def sub_init(self, num):
        '''自循环初始化（获取频率最高的ID）'''
        self.current = set()

        if self.init_flag:
            frequency_table = defaultdict(int)
            with open(self.TARGET_CSV, 'r', encoding='utf8') as f:
                for row in csv.reader(f):
                    frequency_table[row[0]] += 1
                    frequency_table[row[1]] += 1
            self.current = {max(frequency_table.items(), key=lambda x: x[1])[0]}
            self.fre_current = frequency_table[list(self.current)[0]]

            with open(os.path.join(self.GRAPH, f'graph_0.csv'), 'w', encoding='utf8') as f:
                for row in self.current:
                    f.write(f'{row}\n')
        else:
            with open(self.tmp_graph_file) as f:
                for row in csv.reader(f):
                    self.current |= set(row)

        self.target_dir = os.path.join(self.TARGET, f'target_{num + 1}')
        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir)

        self.graph_dir = os.path.join(self.GRAPH, f'graph_{num}')
        if not os.path.exists(self.graph_dir):
            os.makedirs(self.graph_dir)

        return None

    # @print_cost_time
    def merge_sub_file(self):
        '''合并子文件'''
        flag = True
        graph_csv = f'{self.graph_dir}.csv'
        target_csv = f'{self.target_dir}.csv'
        graph_write_f = open(graph_csv, 'w', encoding='utf8')
        target_write_f = open(target_csv, 'w', encoding='utf8')

        # 汇总连通节点
        for file in os.listdir(self.graph_dir):
            with open(os.path.join(self.graph_dir, file), 'r', encoding='utf8') as graph_f:
                for line in graph_f:
                    graph_write_f.write(f'{line.strip()}\n')
        shutil.rmtree(self.graph_dir)
        graph_write_f.close()

        # 汇总不连通节点
        for file in os.listdir(self.target_dir):
            with open(os.path.join(self.target_dir, file), 'r', encoding='utf8') as target_f:
                for line in target_f:
                    target_write_f.write(f'{line.strip()}\n')
        shutil.rmtree(self.target_dir)
        target_write_f.close()

        tmp = self.current_file
        if tmp != self.TARGET_CSV:
            subprocess.getstatusoutput(f'rm -rf {tmp}')
        self.current_file = target_csv
        #subprocess.getstatusoutput(f'rm -rf {self.tmp_graph_file}')

        self.init_flag = False

        self.previous |= self.current
        if not os.path.exists(graph_csv) or os.path.getsize(graph_csv) == 0:
            flag = False
        if flag:
            self.tmp_graph_file = graph_csv
        return flag

    @print_cost_time
    def run(self, graph_index):
        '''运行,查找子图'''
        # step1 合并文件
        self.merge_csv()

        # step2 迭代
        flag = True
        num = 1

        # 寻找子图(1次)
        while flag:
            p = multiprocessing.Pool(self.NUMBER)

            # 获取筛选条件
            self.sub_init(num)

            # 切分文件
            files = [f'{self.TMP}/{i}.csv' for i in range(self.NUMBER)]
            fs = [open(k, 'w', encoding='utf8') for k in files]

            with open(self.current_file, 'r', encoding='utf8') as f:
                for index, row in enumerate(csv.reader(f), 2):
                    pos = index % self.NUMBER
                    csv.writer(fs[pos]).writerow(row)
            for f in fs:
                f.close()

            # 每份文件都由子进程进行计算
            for i in range(self.NUMBER):
                p.apply_async(task, args=(i, files[i], self.target_dir, self.graph_dir))
            p.close()
            p.join()

            # 合并文件
            flag = self.merge_sub_file()

            num += 1

        node_count = len(self.previous)
        with open(f'{os.path.dirname(self.GRAPH)}/graph_nodes.csv', 'w', encoding='utf8') as f:
            for i in self.previous:
                f.write(f'{i}\n')

        return graph_index, node_count, self.fre_current

    def clean(self, graph_index):
        '''清理上次遗留的文件'''
        shutil.rmtree(os.path.join(self.ROOT, graph_index, 'tmp'))
        shutil.rmtree(os.path.join(self.ROOT, graph_index, 'target'))
        shutil.rmtree(os.path.join(self.ROOT, graph_index, 'graph'))
        return None

    def init(self, index):
        '''循环初始化(设置存储路径)'''
        self.TMP = f'/home/20200220csv/tmp/{index}/tmp'
        if not os.path.exists(self.TMP):
            os.makedirs(self.TMP)

        self.TARGET = f'/home/20200220csv/tmp/{index}/target'
        if not os.path.exists(self.TARGET):
            os.makedirs(self.TARGET)

        self.GRAPH = f'/home/20200220csv/tmp/{index}/graph'
        if not os.path.exists(self.GRAPH):
            os.makedirs(self.GRAPH)

        self.init_flag = True

        self.TARGET_CSV = self.current_file
        self.previous = set()  # 前次迭代的连通节点
        self.current = set()   # 当前迭代的联通节点
        self.tmp_graph_file = None  # 上一次迭代的连通节点
        self.target_dir = None
        self.graph_dir = None
        self.init_flag = True

        subprocess.getstatusoutput(f'cp {self.current_file} {os.path.join(self.TARGET, "target_1.csv")}')
        self.clean(index - 1)
        return None

    def main(self):
        '''获取所有的子图'''
        index = 1
        while True:
            if index > 1:
                self.init(index)
            self.run(index)
            if not os.path.exists(self.current_file) or os.path.getsize(self.current_file) == 0:
                break

        print('找找所有的图完成！')
        return None

    @print_cost_time
    def small_text_test(self):
        '''小文件测试'''
        files = []
        for file, label in self.FILES:
            tmp_file = os.path.join(os.path.dirname(file), f'tmp_{os.path.basename(file)}')
            files.append((tmp_file, label))

            read_f = open(file, 'r', encoding='utf8')
            write_f = open(tmp_file, 'w', encoding='utf8')
            index = 1
            for line in read_f:
                if index % 1000 == 1:
                    write_f.write(f'{line.strip()}\n')
                index += 1
            read_f.close()
            write_f.close()
        self.FILES = files
        return None


def task(self, pos, file, target_dir, graph_dir):
    '''
    筛选
    :param num:
    :param pos:
    :param file:
    :return:
    '''
    target_file = os.path.join(target_dir, f'{pos}.csv')    # 不连通节点文件
    graph_file = os.path.join(graph_dir, f'{pos}.csv')          # 连通节点文件
    target_f = open(target_file, 'w', encoding='utf8')
    graph_f = open(graph_file, 'w', encoding='utf8')
    with open(file, 'r', encoding='utf8') as f:
        for row in csv.reader(f):
            flag = False
            for i in row:
                if i in self.current:
                    flag = True
                    break
            if flag:
                for i in row:
                    if i not in self.current:
                        graph_f.write(f'{i}\n')
            else:
                target_f.write(f"{','.join(row)}\n")
    target_f.close()
    graph_f.close()
    return None


if __name__ == '__main__':
    obj = SearchSubgraph()
    #obj.small_text_test()
    obj.run()
    # SearchSubgraph().test(123)
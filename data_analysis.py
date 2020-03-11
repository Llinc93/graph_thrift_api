import csv
import time
from itertools import combinations
from collections import defaultdict


class DataAnalysis(object):
    '''数据分析'''

    CSV_MAP = [
        ('qiyefenzhi.csv', 'BEE'),
        ('qiyetouzi.csv', 'IPEES'),
        ('ziranrentouzi.csv', 'IPEER'),
    ]

    def __init__(self):
        self.frequency_table = {}
        self.report = defaultdict(int)
        self.file_handles = []

    def ipeer(self, f, flag=True):
        for row in f:
            if len(row[0]) != len(row[-1]) != 32:
                continue
            if flag:
                self.frequency_table[row[-1]] = set()
            else:
                self.frequency_table[row[-1]].add(row[0])
                self.frequency_table[row[-1]].add(row[1])
        return None

    def ipees(self, f, flag=True):
        for row in f:
            if len(row[0]) != len(row[-1]) != 32:
                continue

            if flag:
                self.frequency_table[row[0]] = set()
                self.frequency_table[row[-1]] = set()
            else:
                self.frequency_table[row[-1]].add(row[0])
                self.frequency_table[row[-1]].add(row[1])
        return None

    def bee(self, f, flag=True):
        for row in f:
            if len(row[0]) != len(row[1]) != 32:
                continue

            if flag:
                self.frequency_table[row[0]] = set()
                self.frequency_table[row[1]] = set()
            else:
                self.frequency_table[row[0]].add(row[0])
                self.frequency_table[row[0]].add(row[1])
        return None

    def analysis(self, flag=True):
        '''获取全部LCID'''
        for file, code in self.CSV_MAP:
            if hasattr(self, code.lower()):
                with open(file, 'r', encoding='utf8') as read_f:
                    csv_f = csv.reader(read_f)
                    getattr(self, code.lower())(csv_f, flag)
        return None

    def summary(self):
        '''以LCID为基础，进行排列组合，最终获得所有的子图的节点数量'''
        flag = False
        while flag:
            count = 0
            sum_count = 0
            for lcid1, lcid2 in combinations(self.frequency_table.keys(), 2):
                tmp = self.frequency_table[lcid1] | self.frequency_table[lcid2]
                if len(tmp) == len(self.frequency_table[lcid1]) + len(self.frequency_table[lcid2]):
                    count += 1
                    continue
                self.frequency_table[lcid1] = tmp
                self.frequency_table.pop(lcid2)
                sum_count += 1
            if count == sum_count:
                flag = False
        return None

    def gen_report(self):
        for key, value in self.frequency_table.items():
            self.report[len(value)] += 1

        with open('report.txt', 'w', encoding='utf8') as f:
            for key, value in self.report.items():
                f.write(f'{key}个节点的子图数量：{value}')
        return None

    def run(self):
        s1 = time.time()
        self.analysis()
        s2 = time.time()
        print(f'分析耗时：{s2 - s1}秒')

        self.analysis(False)
        s3 = time.time()
        print(f'聚合耗时：{s3 - s2}秒')

        self.summary()
        s4 = time.time()
        print(f'汇总耗时：{s4 - s3}秒')

        self.gen_report()
        print(f'生成报告耗时: {time.time() - s4}秒')
        print('END')

if __name__ == '__main__':
    DataAnalysis().run()
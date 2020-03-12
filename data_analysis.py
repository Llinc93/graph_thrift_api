import csv
import json
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
        self.test_csv = []

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
        flag = True
        index = 1
        while flag:
            print(len(self.frequency_table.keys()))
            count = 0
            sum_count = 0
            filter = []
            for lcid1, lcid2 in combinations(self.frequency_table.keys(), 2):
                if len(self.frequency_table[lcid1]) == 0 or len(self.frequency_table[lcid2]) == 0:
                    if len(self.frequency_table[lcid1]) == 0:
                        filter.append(lcid1)
                    else:
                        filter.append(lcid2)
                    continue

                tmp = self.frequency_table[lcid1] | self.frequency_table[lcid2]
                if len(tmp) == len(self.frequency_table[lcid1]) + len(self.frequency_table[lcid2]):
                    count += 1
                    continue
                self.frequency_table[lcid1] = tmp
                self.frequency_table.pop(lcid2)
                sum_count += 1
            for i in filter:
                self.frequency_table.pop(i)
            if count == sum_count:
                flag = False
            if index % 50 == 1:
                with open(f'data_{index}.json', 'w', encoding='utf8') as f:
                    f.write(json.dumps(self.frequency_table, ensure_ascii=False))
        return None

    def gen_report(self):
        tmp = defaultdict(int)
        for key, value in self.frequency_table.items():
            self.report[len(value)] += 1

        for key, value in self.report.items():
            tmp[value] += key

        with open('report_sum.txt', 'w', encoding='utf8') as f:
            sort_tmp = sorted(tmp.items(), key=lambda x: x[1])
            for key, value in sort_tmp:
                f.write(f'{value}：{key}')

        with open('report.txt', 'w', encoding='utf8') as f:
            sort_tmp2 = sorted(self.report.items(), key=lambda x: x[1])
            for key, value in self.report.items():
                f.write(f'{key}个节点的子图数量：{value}\n')
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
        return None

    def extract(self):
        for file, code in self.CSV_MAP:
            index = 1
            with open(file, 'r', encoding='utf8') as read_f:
                with open(f'{file}_test.csv', 'w', encoding='utf8') as write_f:
                    for line in read_f:
                        if index % 1000 == 1:
                            write_f.write(f'{line.strip()}\n')
            self.test_csv.append((f'{file}_test.csv', code))
        self.CSV_MAP = self.test_csv
        return None

    def test(self):
        self.extract()
        self.run()


if __name__ == '__main__':
    # 试运行
    DataAnalysis().test()

    # 正式运行
    # DataAnalysis().run()

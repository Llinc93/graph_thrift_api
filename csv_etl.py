import csv
import hashlib


class Filter(object):

    files = [
        ('/opt/csv/企业投资.csv', r'/home/neo4j/import/企业投资.csv', 'IPEE', '企业投资'),
        ('/opt/csv/自然人投资.csv', r'/home/neo4j/import/自然人投资.csv', 'IPEE', '股东投资'),
        ('/opt/csv/基本信息企业节点.csv', r'/home/neo4j/import/基本信息企业节点.csv', 'GS', '企业节点'),
        ('/opt/csv/人员节点.csv', r'/home/neo4j/import/人员节点.csv', 'GR', '人员节点'),
        ('/opt/csv/企业分支.csv', r'/home/neo4j/import/企业分支.csv', 'BEE', '分支机构'),
        ('/opt/csv/主要管理人员.csv', r'/home/neo4j/import/主要管理人员.csv', 'SPE', '人员任职'),
        ('/opt/csv/专利_20200221.csv', r'/home/neo4j/import/专利_20200221.csv', 'PP', '专利节点'),
        ('/opt/csv/法律文书.csv', r'/home/neo4j/import/法律文书.csv', 'LL', '诉讼节点'),
        ('/opt/csv/招投标.csv', r'/home/neo4j/import/招投标.csv', 'GB', '招投标节点'),
        ('/opt/csv/相同办公地_年报.csv', r'/home/neo4j/import/相同办公地_年报.csv', 'DD', '办公地节点'),
        ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j/import/相同联系方式_年报.csv', 'TT', '电话节点'),
    ]

    def IPEE(self, row):
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GS(self, row):
        flag = True
        text = ''
        action = row[0:2]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GR(self, row):
        flag = True
        text = ''
        if '' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def BEE(self, row):
        flag = True
        text = ''
        if '' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def SPE(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def PP(self, row):
        flag = True
        text = ''
        action = [row[0], row[1], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def LL(self, row):
        flag = True
        text = ''
        action = [row[0], row[2], row[3]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GB(self, row):
        flag = True
        text = ''
        action = [row[0], row[2], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def DD(self, row):
        flag = True
        text = ''
        action = [row[0], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def TT(self, row):
        flag = True
        text = ''
        action = [row[0], row[2], row[3]]
        if '' in action or '0' in action or '-' in action or '无' in action or '--' in action or '无无' in action or '0000' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def run(self):
        for read, write, label in self.files:
            read_f = open(read, 'r', encoding='utf8')
            write_f = open(write, 'r', encoding='utf8')
            writer = csv.writer(write_f)
            raw = 0
            number = 0
            data = set()
            for line in csv.reader(read_f):
                raw += 1
                # step1 过滤
                flag, text = getattr(self, label)(line)
                if not flag:
                    continue

                # step2 去重
                md5 = hashlib.md5(text.encode('utf8')).hexdigest()
                if md5 not in data:
                    writer.writerow(line)
                    number += 1
                    data.add(md5)
        return None


if __name__ == '__main__':
    Filter().run()
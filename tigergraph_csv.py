import os
import csv
import hashlib
from collections import defaultdict


class Filter(object):

    tmp_files = [
        ('/home/csvdata/基本信息企业节点.csv', r'/home/neo4j_test/import/基本信息企业节点.csv', 'GS'),
        ('/home/csvdata/人员节点-投资.csv', r'/home/neo4j_test/import/人员节点-投资.csv', 'GR'),
        ('/home/csvdata/人员节点-高管.csv', r'/home/neo4j_test/import/人员节点-高管.csv', 'GR'),
        ('/opt/csv/jsdata/企业投资_0414.csv', r'/home/neo4j_test/import/企业投资.csv', 'IPEES'),
        ('/home/csvdata/自然人投资_0309.csv', r'/home/neo4j_test/import/自然人投资.csv', 'IPEER'),
        ('/home/csvdata/企业分支.csv', r'/home/neo4j_test/import/企业分支.csv', 'BEE'),
        ('/home/csvdata/主要管理人员position处理0311.csv', r'/home/neo4j_test/import/主要管理人员.csv', 'SPE'),

        ('/home/csvdata/专利关系.csv', r'/home/neo4j_test/import/专利关系.csv', 'OPEP'),
        ('/home/csvdata/专利节点.csv', r'/home/neo4j_test/import/专利节点.csv', 'PP'),

        ('/home/csvdata/诉讼关系.csv', r'/home/neo4j_test/import/诉讼关系.csv', 'LEL'),
        ('/home/csvdata/诉讼节点.csv', r'/home/neo4j_test/import/诉讼节点.csv', 'LL'),

        ('/home/csvdata/招投标关系.csv', r'/home/neo4j_test/import/招投标关系.csv', 'WEB'),
        ('/home/csvdata/招投标节点.csv', r'/home/neo4j_test/import/招投标节点.csv', 'GB'),

        ('/home/csvdata/相同办公地.csv', r'/home/neo4j_test/import/相同办公地.csv', 'RED'),
        ('/home/csvdata/办公地节点.csv', r'/home/neo4j_test/import/办公地节点.csv', 'DD'),

        ('/home/csvdata/相同联系方式-电话0306.csv', r'/home/neo4j_test/import/相同联系方式-电话.csv', 'LEE1'),
        ('/home/csvdata/电话节点.csv', r'/home/neo4j_test/import/电话节点.csv', 'TT'),

        ('/home/csvdata/相同联系方式-邮箱0306.csv', r'/home/neo4j_test/import/相同联系方式-邮箱.csv', 'LEE2'),
        ('/home/csvdata/邮箱节点.csv', r'/home/neo4j_test/import/邮箱节点.csv', 'EE'),
    ]

    files = [
        ('/home/neo4j_test/import/基本信息企业节点.csv', r'/home/neo4j_test/import/gs.csv', 'GS', '企业节点'),
        ('/home/neo4j_test/import/人员节点-投资.csv', r'/home/neo4j_test/import/gri.csv', 'GR', '人员节点'),
        ('/home/neo4j_test/import/人员节点-高管.csv', r'/home/neo4j_test/import/grs.csv', 'GR', '人员节点'),
        ('/home/neo4j_test/import/企业投资.csv', r'/home/neo4j_test/import/ipees.csv', 'IPEES', '投资'),
        ('/home/neo4j_test/import/自然人投资.csv', r'/home/neo4j_test/import/ipeer.csv', 'IPEER', '投资'),
        ('/home/neo4j_test/import/企业分支.csv', r'/home/neo4j_test/import/bee.csv', 'BEE', '人员任职'),
        ('/home/neo4j_test/import/主要管理人员.csv', r'/home/neo4j_test/import/spe.csv', 'SPE', '专利节点'),
        ('/home/neo4j_test/import/专利节点.csv', r'/home/neo4j_test/import/pp.csv', 'PP', '专利关系'),
        ('/home/neo4j_test/import/专利关系.csv', r'/home/neo4j_test/import/opep.csv', 'OPEP', '专利关系'),
        ('/home/neo4j_test/import/诉讼节点.csv', r'/home/neo4j_test/import/ll.csv', 'LL', '诉讼节点'),
        ('/home/neo4j_test/import/诉讼关系.csv', r'/home/neo4j_test/import/lel.csv', 'LEL', '诉讼关系'),
        ('/home/neo4j_test/import/招投标节点.csv', r'/home/neo4j_test/import/gb.csv', 'GB', '招投标节点'),
        ('/home/neo4j_test/import/招投标关系.csv', r'/home/neo4j_test/import/web.csv', 'WEB', '招投标关系'),
        ('/home/neo4j_test/import/办公地节点.csv', r'/home/neo4j_test/import/dd.csv', 'DD', '办公地节点'),
        ('/home/neo4j_test/import/相同办公地.csv', r'/home/neo4j_test/import/red.csv', 'RED', '相同办公地'),
        ('/home/neo4j_test/import/电话节点.csv', r'/home/neo4j_test/import/tt.csv', 'TT', '电话节点'),
        ('/home/neo4j_test/import/相同联系方式-电话.csv', r'/home/neo4j_test/import/lee1.csv', 'LEET', '相同联系方式'),
        ('/home/neo4j_test/import/邮箱节点.csv', r'/home/neo4j_test/import/ee.csv', 'EE', '邮箱节点'),
        ('/home/neo4j_test/import/相同联系方式-邮箱.csv', r'/home/neo4j_test/import/le2.csv', 'LEEE', '相同联系方式'),
    ]

    def __init__(self):
        self.filter = defaultdict(int)
        self.report = {}

    def get_id(self, name):
        return hashlib.md5(name.encode('utf8')).hexdigest()

    def GS(self, row, ignore=False):
        '''
        LCID、ENTNAME
        :param row:
        :return:
        '''
        if ignore:
            row.append('GS')
            return row
        flag = True
        text = ''
        action = [row[0], row[1]]
        if '' in action or len(row) != 9:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GR(self, row, ignore=False):
        '''
        PID、NAME
        :param row:
        :return:
        '''
        if ignore:
            row.append('GR')
            return row
        flag = True
        text = ''
        if '' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def IPEES(self, row, ignore=False):
        '''
        LCID_INV、LCID
        :param row:
        :return:
        '''
        if ignore:
            return [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEE']
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action or len(row) != 4:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def IPEER(self, row, ignore=False):
        '''
        PID_INV、LCID
        :param row:
        :return:
        '''
        if ignore:
            return [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEE']
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action or len(row) != 4:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def BEE(self, row, ignore=False):
        '''
        B_LCID、P_LCID
        :param row:
        :return:
        '''
        if ignore:
            row.append('BEE')
            return row
        flag = True
        text = ''
        if '' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def SPE(self, row, ignore=False):
        '''
        LCID、POSITION、PID
        :param row:
        :return:
        '''
        if ignore:
            row.append('SPE')
            return row
        flag = True
        text = ''
        if '' in row or 'null' in row or len(row) != 3:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def PP(self, row, ignore=False):
        '''
        FZL_MC、FZL_SQH、LCID
        :param row:
        :return:
        '''
        if ignore:
            return [row[1], row[0], row[1], 'PP']
        flag = True
        text = ''

        if '' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def OPEP(self, row, ignore=False):
        if ignore:
            return [row[1], row[0], 'OPEP']
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LL(self, row, ignore=False):
        '''
        FFL_TITLE
        :param row:
        :return:
        '''
        if ignore:
            return [self.get_id(row[0]), row[0], 'LL']
        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEL(self, row, ignore=False):
        if ignore:
            return [self.get_id(row[0]), row[1], 'LEL']
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def GB(self, row, ignore=False):
        '''
        FZE_TITLE、LCID
        :param row:
        :return:
        '''
        if ignore:
            return [self.get_id(row[0]), row[0], 'GB']
        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def WEB(self, row, ignore=False):
        if ignore:
            return [row[1], self.get_id(row[0]), 'WEB']
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def DD(self, row, ignore=False):
        '''
        ADDR、LCID
        :param row:
        :return:
        '''
        if ignore:
            return [self.get_id(row[0]), row[0], 'DD']

        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def RED(self, row, ignore=False):
        if ignore:
            return [row[1], self.get_id(row[0]), 'RED']
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def TT(self, row, ignore=False):
        '''
        TEL、LCID、EMAIL
        :param row:
        :return:
        '''
        if ignore:
            return [row[0], row[0], 'TT']
        flag = True
        text = ''

        if '' in row or '0' in row or '-' in row or '无' in row or '--' in row or '无无' in row or '0000' in row or '1' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEET(self, row, ignore=False):
        if ignore:
            return [row[1], row[0], row[2], 'LEE']
        flag = True
        text = ''
        action = [row[0], row[1]]

        if '' in action or '0' in action or '-' in action or '无' in action or '--' in action or '无无' in action or '0000' in action or '1' in action or len(row) != 3:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def EE(self, row, ignore=False):
        if ignore:
            return [row[0], row[0], 'EE']
        flag = True
        text = ''

        if '.' not in row[0] or len(row) != 1 or '@' not in row[0]:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEEE(self, row, ignore=False):
        if ignore:
            return [row[1], row[0], row[2], 'LEE']
        flag = True
        text = ''
        action = [row[0], row[1]]

        if '.' not in row[0] or '@' not in row[0] or len(row) != 3:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def have_multi_relationship(self, s):
        id_md5 = hashlib.md5(s.encode('utf8')).hexdigest()
        return True if self.filter[id_md5] > 1 else False

    def write(self, read, write, label):
        read_f = open(read, 'r', encoding='utf8')
        write_f = open(write, 'w', encoding='utf8')
        writer = csv.writer(write_f)
        raw = 0
        number = 0
        data = set()
        for line in csv.reader(read_f):
            raw += 1
            # step1 过滤
            try:
                flag, text = getattr(self, label)(line)
            except:
                continue
            if not flag:
                continue

            # step2 去重
            md5 = hashlib.md5(text.encode('utf8')).hexdigest()
            if md5 not in data:
                writer.writerow(line)
                number += 1
                data.add(md5)
        read_f.close()
        write_f.close()
        print(f'{read}\t数量: {raw}')
        print(f'{write}\t数量: {number}')
        return None

    def write_with_filter(self, read, write, label):
        read_f = open(read, 'r', encoding='utf8')
        tmp_file = f'{os.path.dirname(read)}/tmp_{os.path.basename(read)}'
        write_f = open(tmp_file, 'w', encoding='utf8')
        writer = csv.writer(write_f)
        data = set()
        if label in ['OPEP', 'LEL', 'WEB', 'RED', 'LEE1', 'LEE2']:
            self.filter = defaultdict(int)

        if label in ['PP']:
            pos = -1
        else:
            pos = 0

        raw = 0
        for line in csv.reader(read_f):
            raw += 1
            # step1 过滤
            try:
                flag, text = getattr(self, label)(line)
            except:
                continue
            if not flag:
                continue

            # step2 去重
            md5 = hashlib.md5(text.encode('utf8')).hexdigest()
            if md5 not in data:
                writer.writerow(line)
                data.add(md5)
                id_md5 = hashlib.md5(line[pos].encode('utf8')).hexdigest()
                self.filter[id_md5] += 1
        read_f.close()
        write_f.close()

        read_f2 = open(tmp_file, 'r', encoding='utf8')
        write_f2 = open(write, 'w', encoding='utf8')
        writer2 = csv.writer(write_f2)
        number = 0
        for line in csv.reader(read_f2):
            if self.have_multi_relationship(line[pos]):
                writer2.writerow(line)
                number += 1
        read_f2.close()
        write_f2.close()
        os.remove(tmp_file)
        print(f'{read}\t数量: {raw}')
        print(f'{write}\t数量: {number}')

    def run(self):

        for read, write, label in self.tmp_files:
            if label not in ['IPEES', 'IPEER', 'GR', 'GS', 'SPE', 'BEE']:
                self.write_with_filter(read, write, label)
            else:
                self.write(read, write, label)

        for read_file, write_file, label, desc in self.files:
            rf = open(read_file, 'r', encoding='utf8')
            wf = open(write_file, 'w', encoding='utf8', newline='')

            index = 1
            for row in csv.reader(rf):
                writer = csv.writer(wf)
                if index == 1:
                    continue
                else:
                    try:
                        new_row = getattr(self, label)(row, True)
                    except:
                        continue
                    writer.writerow([k if k else 'null' for k in new_row])
                index += 1
            else:
                self.report[desc] = index

            rf.close()
            wf.close()


if __name__ == '__main__':
    obj = Filter()
    obj.run()

    # 返回表中数据统计
    for key, value in obj.report.items():
        print(key, value)


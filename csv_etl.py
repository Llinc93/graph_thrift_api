import csv
import hashlib


class Filter(object):

    files = [
        ('/opt/csv/企业投资.csv', r'/home/neo4j_test/import/企业投资.csv', 'IPEE'),
        ('/opt/csv/自然人投资.csv', r'/home/neo4j_test/import/自然人投资.csv', 'IPEE'),
        ('/opt/csv/基本信息企业节点.csv', r'/home/neo4j_test/import/基本信息企业节点.csv', 'GS'),
        ('/opt/csv/人员节点.csv', r'/home/neo4j_test/import/人员节点.csv', 'GR'),
        ('/opt/csv/企业分支.csv', r'/home/neo4j_test/import/企业分支.csv', 'BEE'),
        ('/opt/csv/主要管理人员.csv', r'/home/neo4j_test/import/主要管理人员.csv', 'SPE'),
        ('/opt/csv/专利_20200228.csv', r'/home/neo4j_test/import/专利_20200221.csv', 'PP'),
        ('/opt/csv/法律文书.csv', r'/home/neo4j_test/import/法律文书.csv', 'LL'),
        ('/opt/csv/招投标.csv', r'/home/neo4j_test/import/招投标.csv', 'GB'),
        ('/opt/csv/相同办公地_年报.csv', r'/home/neo4j_test/import/相同办公地_年报.csv', 'DD'),
        ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j_test/import/相同联系方式_年报.csv', 'TT'),
    ]

    def IPEE(self, row):
        '''
        PID_INV/LCID_INV、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GS(self, row):
        '''
        LCID、ENTNAME
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = row[0:2]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GR(self, row):
        '''
        PID、NAME
        :param row:
        :return:
        '''
        flag = True
        text = ''
        if '' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def BEE(self, row):
        '''
        B_LCID、P_LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        if '' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def SPE(self, row):
        '''
        LCID、POSITION、PID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def PP(self, row):
        '''
        FZL_MC、FZL_SQH、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[1], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def LL(self, row):
        '''
        FFL_TITLE、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def GB(self, row):
        '''
        FZE_TITLE、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def DD(self, row):
        '''
        ADDR、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def TT(self, row):
        '''
        TEL、LCID、EMAIL
        :param row:
        :return:
        '''
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


if __name__ == '__main__':
    Filter().run()

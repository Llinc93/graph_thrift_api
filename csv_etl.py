import os
import csv
import hashlib
from collections import defaultdict


class Filter(object):

    files = [
        ('/home/csv/gs-0602.csv', r'/home/neo4j-1/import/tmp-gs.csv', 'GS'),
        ('/home/csv/gri-0602.csv', r'/home/neo4j-1/import/tmp-gri.csv', 'GR'),
        ('/home/csv/grs-0602.csv', r'/home/neo4j-1/import/tmp-grs.csv', 'GR'),
        ('/home/csv/ipees-0602.csv', r'/home/neo4j-1/import/tmp-ipees.csv', 'IPEE'),
        ('/home/csv/ipeer-0602.csv', r'/home/neo4j-1/import/tmp-ipeer.csv', 'IPEE'),
        ('/home/csv/bee-0602.csv', r'/home/neo4j-1/import/tmp-bee.csv', 'BEE'),
        ('/home/csv/spe-0602.csv', r'/home/neo4j-1/import/tmp-spe.csv', 'SPE'),

        ('/home/csv/opep-0602.csv', r'/home/neo4j-1/import/tmp-opep.csv', 'OPEP'),
        ('/home/csv/pp-0602.csv', r'/home/neo4j-1/import/tmp-pp.csv', 'PP'),

        ('/home/csv/lel-0602.csv', r'/home/neo4j-1/import/tmp-lel.csv', 'LEL'),
        ('/home/csv/ll-0602.csv', r'/home/neo4j-1/import/tmp-ll.csv', 'LL'),

        ('/home/csv/web-0602.csv', r'/home/neo4j-1/import/tmp-web.csv', 'WEB'),
        ('/home/csv/gb-0602.csv', r'/home/neo4j-1/import/tmp-gb.csv', 'GB'),

        ('/home/csv/red-0602.csv', r'/home/neo4j-1/import/tmp-red.csv', 'RED'),
        ('/home/csv/dd-0602.csv', r'/home/neo4j-1/import/tmp-dd.csv', 'DD'),

        ('/home/csv/leet-0602.csv', r'/home/neo4j-1/import/tmp-leet.csv', 'LEET'),
        ('/home/csv/tt-0602.csv', r'/home/neo4j-1/import/tmp-tt.csv', 'TT'),

        ('/home/csv/leee-0602.csv', r'/home/neo4j-1/import/tmp-leee.csv', 'LEEE'),
        ('/home/csv/ee-0602.csv', r'/home/neo4j-1/import/tmp-ee.csv', 'EE'),
    ]

    def GS(self, row):
        '''
        LCID、ENTNAME
        :param row:
        :return:
        '''
        flag = True
        text = ''
        action = [row[0], row[1]]
        # if '' in action or len(row) != 9:
        if len(row) != 9:
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
        # if '' in row or len(row) != 2:
        if len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def IPEE(self, row):
        flag = True
        text = ''
        action = [row[0], row[2]]
        if '' in action or len(action) != 4:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def BEE(self, row):
        '''
        B_LCID、P_LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''
        if '' in row or len(row) != 2:
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
        if '' in row or 'null' in row or len(row) != 3:
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

        if '' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def OPEP(self, row):
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LL(self, row):
        '''
        FFL_TITLE
        :param row:
        :return:
        '''
        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEL(self, row):
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def GB(self, row):
        '''
        FZE_TITLE、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def WEB(self, row):
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def DD(self, row):
        '''
        ADDR、LCID
        :param row:
        :return:
        '''
        flag = True
        text = ''

        if '' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def RED(self, row):
        flag = True
        text = ''

        if '' in row or 'null' in row or len(row) != 2:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def TT(self, row):
        '''
        TEL、LCID、EMAIL
        :param row:
        :return:
        '''
        flag = True
        text = ''

        if '' in row or '0' in row or '-' in row or '无' in row or '--' in row or '无无' in row or '0000' in row or '1' in row or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEET(self, row):
        flag = True
        text = ''
        action = [row[0], row[1]]

        if '' in action or '0' in action or '-' in action or '无' in action or '--' in action or '无无' in action or '0000' in action or '1' in action or len(row) != 3:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def EE(self, row):
        flag = True
        text = ''

        if '.' not in row[0] or len(row) != 1 or '@' not in row[0] or len(row) != 1:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEEE(self, row):
        flag = True
        text = ''
        action = [row[0], row[1]]

        if '.' not in row[0] or '@' not in row[0] or len(row) != 3:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

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
            flag, text = getattr(self, label)(line)
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

    def run(self):
        for read, write, label in self.files:
            self.write(read, write, label)
        return None


if __name__ == '__main__':
    Filter().run()
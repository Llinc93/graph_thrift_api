# coding: utf8
import os
import hashlib
import subprocess
import csv


report = {}

GS_header = ['ID:ID(ENT-ID)', 'NAME', 'UNISCID', 'ESDATE', 'INDUSTRY', 'PROVINCE', 'REGCAP', 'RECCAPCUR', 'ENTSTATUS', 'label', ':LABEL']
GR_header = ['ID:ID(P-ID)', 'NAME', 'label', ':LABEL']

IPEER_header = ['ID:START_ID(P-ID)', 'RATE', 'RATE_TYPE', 'ID', 'pid', 'id', 'label', 'ID:END_ID(ENT-ID)', ':TYPE']
IPEES_header = ['ID:START_ID(ENT-ID)', 'RATE', 'RATE_TYPE', 'ID', 'pid', 'id', 'label', 'ID:END_ID(ENT-ID)', ':TYPE']
BEE_header = ['ID:END_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'RATE', 'ID:START_ID(ENT-ID)', ':TYPE']
SPE_header = ['ID:END_ID(ENT-ID)', 'POSITION', 'ID', 'pid', 'id', 'label', 'ID:START_ID(P-ID)', ':TYPE']

PP_header = ['ID:ID(FZL-ID)', 'NAME', 'FZL_SQH', 'label', ':LABEL']
OPEP_header = ['ID:START_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'ID:END_ID(FZL-ID)', ':TYPE']

LL_header = ['ID:ID(FFL-ID)', 'NAME', 'label', ':LABEL']
LEL_header = ['ID:END_ID(FFL-ID)', 'ID', 'pid', 'id', 'label', 'ID:START_ID(ENT-ID)', ':TYPE']

GB_header = ['ID:ID(FZE-ID)', 'NAME', 'label', ':LABEL']
WEB_header = ['ID:START_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'ID:END_ID(FZE-ID)', ':TYPE']

DD_header = ['ID:ID(ADDR-ID)', 'NAME', 'label', ':LABEL']
RED_header = ['ID:START_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'ID:END_ID(ADDR-ID)', ':TYPE']

TT_header = ['ID:ID(TEL-ID)', 'NAME', 'label', ':LABEL']
LEET_header = ['ID:START_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'ID:END_ID(TEL-ID)', 'DOMAIN', ':TYPE']

EE_header = ['ID:ID(EMAIL-ID)', 'NAME', 'label', ':LABEL']
LEEE_header = ['ID:START_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'ID:END_ID(EMAIL-ID)', 'DOMAIN', ':TYPE']

files = [
    ('/home/neo4j/import/基本信息企业节点.csv', r'/home/neo4j/import/gs.csv', GS_header, 'GS', '企业节点'),
    ('/home/neo4j/import/人员节点-投资.csv', r'/home/neo4j/import/gri.csv', GR_header, 'GR', '人员节点'),
    ('/home/neo4j/import/人员节点-高管.csv', r'/home/neo4j/import/grs.csv', GR_header, 'GR', '人员节点'),
    ('/home/neo4j/import/企业投资.csv', r'/home/neo4j/import/ipees.csv', IPEES_header, 'IPEES', '投资'),
    ('/home/neo4j/import/自然人投资.csv', r'/home/neo4j/import/ipeer.csv', IPEER_header, 'IPEER', '投资'),
    ('/home/neo4j/import/企业分支.csv', r'/home/neo4j/import/bee.csv', BEE_header, 'BEE', '人员任职'),
    ('/home/neo4j/import/主要管理人员.csv', r'/home/neo4j/import/spe.csv', SPE_header, 'SPE', '专利节点'),
    ('/home/neo4j/import/专利节点.csv', r'/home/neo4j/import/pp.csv', PP_header, 'PP', '专利关系'),
    ('/home/neo4j/import/专利关系.csv', r'/home/neo4j/import/opep.csv', OPEP_header, 'OPEP', '专利关系'),
    ('/home/neo4j/import/诉讼节点.csv', r'/home/neo4j/import/ll.csv', LL_header, 'LL', '诉讼节点'),
    ('/home/neo4j/import/诉讼关系.csv', r'/home/neo4j/import/lel.csv', LEL_header, 'LEL', '诉讼关系'),
    ('/home/neo4j/import/招投标节点.csv', r'/home/neo4j/import/gb.csv', GB_header, 'GB', '招投标节点'),
    ('/home/neo4j/import/招投标关系.csv', r'/home/neo4j/import/web.csv', WEB_header, 'WEB', '招投标关系'),
    ('/home/neo4j/import/办公地节点.csv', r'/home/neo4j/import/dd.csv', DD_header, 'DD', '办公地节点'),
    ('/home/neo4j/import/相同办公地.csv', r'/home/neo4j/import/red.csv', RED_header, 'RED', '相同办公地'),
    ('/home/neo4j/import/电话节点.csv', r'/home/neo4j/import/tt.csv', TT_header, 'TT', '电话节点'),
    ('/home/neo4j/import/相同联系方式-电话.csv', r'/home/neo4j/import/leeT.csv', LEET_header, 'LEET', '相同联系方式'),
    ('/home/neo4j/import/邮箱节点.csv', r'/home/neo4j/import/ee.csv', EE_header, 'EE', '邮箱节点'),
    ('/home/neo4j/import/相同联系方式-邮箱.csv', r'/home/neo4j/import/leee.csv', LEEE_header, 'LEEE', '相同联系方式'),
]


class WriteCSV(object):
    '''转化成neo4j需要的格式文件'''

    def get_id(self, name):
        return hashlib.md5(name.encode('utf8')).hexdigest()

    def GS(self, row):
        row.append('GS')
        row.append('GS')
        return row

    def GR(self, row):
        row.append('GR')
        row.append('GR')
        return row

    def IPEES(self, row):
        tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEES']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[0], row[1] if row[1] else 0, row[-1], rowkey, row[0], row[2], 'IPEES', row[2], 'IPEES']

    def IPEER(self, row):
        tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEER']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[0], row[1] if row[1] else 0, row[-1], rowkey, row[0], row[2], 'IPEER', row[2], 'IPEER']

    def BEE(self, row):
        row.append('BEE')
        tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEER']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[0], rowkey, row[1], row[0], 1, 'BEE', row[1], 'BEE']

    def SPE(self, row):
        row.append('SPE')
        tmp = [row[0], row[1], row[2], 'SPE']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[0], row[1], rowkey, row[2], row[0], 'SPE', row[2], 'SPE']

    def PP(self, row):
        return [row[1], row[0], row[1], 'PP', 'PP']

    def OPEP(self, row):
        tmp = [row[1], row[0], 'OPEP']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[1], rowkey, row[1], row[0], 'OPEP', row[0], ':TYPE']

    def LL(self, row):
        return [self.get_id(row[0]), row[0], 'LL', 'LL']

    def LEL(self, row):
        end_id = self.get_id(row[0])
        tmp = [end_id, row[1], 'LEL']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [end_id, rowkey, end_id, end_id, 'LEL', row[1], 'LEL']

    def GB(self, row):
        return [self.get_id(row[0]), row[0], 'GB', 'GB']

    def WEB(self, row):
        end_id = self.get_id(row[0])
        tmp = [row[1], end_id, 'WEB']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[1], rowkey, row[1], end_id, 'WEB', end_id, 'WEB']

    def DD(self, row):
        return [self.get_id(row[0]), row[0], 'DD', 'DD']

    def RED(self, row):
        tmp = [row[1], row[0], 'OPEP']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[1], rowkey, row[1],  row[0], 'OPEPFF', row[0], 'OPEP']

    def TT(self, row):
        return [row[0], row[0], 'TT', 'TT']

    def LEET(self, row):
        tmp = [row[1], row[0], row[2], 'LEE']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[1], rowkey, row[1], row[0], 'LEE', row[0], row[2], 'LEE']

    def EE(self, row):
        return [row[0], row[0], 'EE', 'EE']

    def LEEE(self, row):
        tmp = [row[1], row[0], row[2], 'LEE']
        rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
        return [row[1], rowkey, row[1], row[0], 'LEE', row[0], row[2], 'LEE']


def run():
    w_csv = WriteCSV()
    for read_file, write_file, header, label, desc in files:
        rf = open(read_file, 'r', encoding='utf8')
        wf = open(write_file, 'w', encoding='utf8', newline='')

        index = 1
        for row in csv.reader(rf):
            writer = csv.writer(wf)
            if index == 1:
                writer.writerow(header)
            else:
                try:
                    new_row = getattr(w_csv, label)(row)
                except:
                    continue
                writer.writerow([k if k else 'null' for k in new_row])
            index += 1
        else:
            report[desc] = index

        rf.close()
        wf.close()
    return None


if __name__ == '__main__':
    run()
    # 删除原有的数据库，导入新的数据库
    rm_cmd = f'rm -rf /home/neo4j_test/data/databases/graph.db'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)

    import_cmd = "docker exec -it neo4j_test /bin/bash -c 'bin/neo4j-admin import " \
                 "--nodes=import/gs.csv " \
                 "--nodes=import/gri.csv " \
                 "--nodes=import/grs.csv " \
                 "--nodes=import/pp.csv " \
                 "--nodes=import/ll.csv " \
                 "--nodes=import/gb.csv " \
                 "--nodes=import/dd.csv " \
                 "--nodes=import/tt.csv " \
                 "--nodes=import/ee.csv " \
                 "--relationships=import/ipees.csv " \
                 "--relationships=import/ipeer.csv " \
                 "--relationships=import/bee.csv " \
                 "--relationships=import/spe.csv " \
                 "--relationships=import/opep.csv " \
                 "--relationships=import/lel.csv " \
                 "--relationships=import/web.csv " \
                 "--relationships=import/red.csv " \
                 "--relationships=import/leet.csv " \
                 "--relationships=import/leee.csv " \
                 "--ignore-missing-nodes --ignore-duplicate-nodes --high-io=true'"
    os.system(import_cmd)
    os.system('docker restart neo4j_test')



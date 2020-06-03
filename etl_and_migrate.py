import os
import csv
import hashlib
import subprocess
from multiprocessing import Pool, cpu_count


class EtlMigrate(object):

    def GS(self, row, md5_id=None):
        '''
        "LCID", ENTNAME", UNISCID, ESDATE, INDUSTRY", PROVINCE, REGCAP, RECCAPCUR, ENTSTATUS
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''
            action = [row[0], row[1]]
            if '' in action or len(row) != 9:
                flag = False
            else:
                text = ','.join(action)
            return flag, text
        else:
            row.append('GS')
            row.append('GS')
            return row

    def GR(self, row, md5_id=None):
        '''
        PID, NAME
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''
            if '' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            row.append('GR')
            row.append('GR')
            return row

    def IPEES(self, row, md5_id=None):
        '''
        LCID_INV, RATE, LCID, RATE_TYPE
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''
            tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEES']

            if '' in row or len(row) != 4:
                flag = False
            else:
                text = ','.join(tmp)
            return flag, text
        else:
            tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEES']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[0], row[1] if row[1] else 0, row[-1], rowkey, row[0], row[2], 'IPEES', row[2], 'IPEES']

    def IPEER(self, row, md5_id=None):
        '''
        PID_INV, RATE, LCID, RATE_TYPE
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''
            tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEER']

            if '' in row or len(row) != 4:
                flag = False
            else:
                text = ','.join(tmp)
            return flag, text
        else:
            tmp = [row[0], row[1] if row[1] else 0, row[-1], row[2], 'IPEES']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[0], row[1] if row[1] else 0, row[-1], rowkey, row[0], row[2], 'IPEER', row[2], 'IPEER']

    def BEE(self, row, md5_id=None):
        '''
        B_LCID, P_LCID
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[0], row[1], 'BEE']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[0], rowkey, row[1], row[0], 'BEE', 1, row[1], 'BEE']

    def SPE(self, row, md5_id=None):
        '''
        LCID, PID, POSITION
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or 'null' in row or len(row) != 3:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[0], row[2], row[1], 'SPE']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[0], row[2], rowkey, row[1], row[0], 'SPE', row[1], 'SPE']

    def PP(self, row, md5_id=None):
        '''
        FZL_MC, FZL_SQH
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [row[1], row[0], 'PP', 'PP']

    def OPEP(self, row, md5_id=None):
        '''
        FZL_SQH, LCID
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or 'null' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[1], row[0], 'OPEP']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[1], rowkey, row[1], row[0], 'OPEP', row[0], ':OPEP']

    def LL(self, row, md5_id=None):
        '''
        FFL_TITLE
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or len(row) != 1:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [self.get_id(row), row[0], 'LL', 'LL']

    def LEL(self, row, md5_id=None):
        '''
        FFL_TITLE, LCID
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or 'null' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[0], row[1], 'LEL']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [md5_id, rowkey, row[1], md5_id, 'LEL', row[1], 'LEL']

    def GB(self, row, md5_id=None):
        '''
        FZE_TITLE
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or len(row) != 1:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [self.get_id(row), row[0], 'GB', 'GB']

    def WEB(self, row, md5_id=None):
        '''
        FZE_TITLE, LCID
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or 'null' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[1], row[0], 'WEB']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[1], rowkey, row[1], md5_id, 'WEB', md5_id, 'WEB']

    def DD(self, row, md5_id=None):
        '''
        ADDR
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or len(row) != 1:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [self.get_id(row), row[0], 'DD', 'DD']

    def RED(self, row, md5_id=None):
        '''
        ADDR, LCID
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or 'null' in row or len(row) != 2:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[1], row[0], 'OPEP']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[1], rowkey, row[1], md5_id, 'OPEPFF', md5_id, 'OPEP']

    def TT(self, row, md5_id=None):
        '''
        TEL
        :param row:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '' in row or '0' in row or '-' in row or '无' in row or '--' in row or '无无' in row or '0000' in row or '1' in row or len(row) != 1:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [row[0], row[0], 'TT', 'TT']

    def LEET(self, row, md5_id=None):
        '''
        TEL, LCID, DOMAIN
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''
            action = [row[0], row[1]]

            if '' in action or '0' in action or '-' in action or '无' in action or '--' in action or '无无' in action or '0000' in action or '1' in action or len(row) != 3:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[1], row[0], row[2], 'LEE']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[1], rowkey, row[1], row[0], 'LEE', row[0], row[2], 'LEE']

    def EE(self, row, md5_id=None):
        '''
        EMAIL
        :param row:
        :param md5_id:
        :return:
        '''
        if not md5_id:
            flag = True
            text = ''

            if '.' not in row[0] or '@' not in row[0] or len(row) != 1:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            return [row[0], row[0], 'EE', 'EE']

    def LEEE(self, row, md5_id=None):
        '''
        EMAIL, LCID, DOMAIN
        :param row: 
        :param md5_id: 
        :return: 
        '''
        if not md5_id:
            flag = True
            text = ''

            if '.' not in row[0] or '@' not in row[0] or len(row) != 3:
                flag = False
            else:
                text = ','.join(row)
            return flag, text
        else:
            tmp = [row[1], row[0], row[2], 'LEE']
            rowkey = hashlib.md5(','.join(tmp).encode('utf8')).hexdigest()
            return [row[1], rowkey, row[1], row[0], 'LEE', row[0], row[2], 'LEE']

    def write(self, read, write, header, label, desc):
        read_f = open(read, 'r', encoding='utf8', newline='')
        write_f = open(write, 'w', encoding='utf8')
        writer = csv.writer(write_f, quoting=csv.QUOTE_ALL)
        raw = 0
        number = 0
        data = set()
        for line in csv.reader(read_f):
            raw += 1
            if raw == 1:
                writer.writerow(header)
                continue

            # step1 过滤
            flag, text = getattr(self, label)(line)
            if not flag:
                continue

            # step2 去重
            md5_id = hashlib.md5(text.encode('utf8')).hexdigest()
            if md5_id not in data:
                row = getattr(self, label)(line, md5_id)
                writer.writerow(row)
                number += 1
                data.add(md5_id)

        read_f.close()
        write_f.close()
        print(f'{desc} {read}\t数量: {raw}')
        print(f'{desc} {write}\t数量: {number}')
        return None

    def get_id(self, name):
        return hashlib.md5(','.join(name).encode('utf8')).hexdigest()


def task(read, write, header, label, desc, migrate_class):
    migrate_class().write(read, write, header, label, desc)
    return None


if __name__ == '__main__':

    GS_header = ['ID:ID(ENT-ID)', 'NAME', 'UNISCID', 'ESDATE', 'INDUSTRY', 'PROVINCE', 'REGCAP', 'RECCAPCUR',
                 'ENTSTATUS', 'label', ':LABEL']
    GR_header = ['ID:ID(P-ID)', 'NAME', 'label', ':LABEL']

    IPEER_header = ['ID:START_ID(P-ID)', 'RATE', 'RATE_TYPE', 'ID', 'pid', 'id', 'label', 'ID:END_ID(ENT-ID)', ':TYPE']
    IPEES_header = ['ID:START_ID(ENT-ID)', 'RATE', 'RATE_TYPE', 'ID', 'pid', 'id', 'label', 'ID:END_ID(ENT-ID)',
                    ':TYPE']
    BEE_header = ['ID:END_ID(ENT-ID)', 'ID', 'pid', 'id', 'label', 'RATE', 'ID:START_ID(ENT-ID)', ':TYPE']
    SPE_header = ['ID:END_ID(ENT-ID)', 'POSITION', 'ID', 'pid', 'id', 'label', 'ID:START_ID(P-ID)', ':TYPE']

    PP_header = ['ID:ID(FZL-ID)', 'NAME', 'label', ':LABEL']
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
        ('/home/csv/gs-0602.csv', r'/home/neo4j-1/import/gs.csv', GS_header, 'GS', '企业节点'),
        ('/home/csv/gri-0602.csv', r'/home/neo4j-1/import/gri.csv', GR_header, 'GR', '人员节点'),
        ('/home/csv/grs-0602.csv', r'/home/neo4j-1/import/grs.csv', GR_header, 'GR', '人员节点'),
        ('/home/csv/ipees-0602.csv', r'/home/neo4j-1/import/ipees.csv', IPEES_header, 'IPEES', '投资'),
        ('/home/csv/ipeer-0602.csv', r'/home/neo4j-1/import/ipeer.csv', IPEER_header, 'IPEER', '投资'),
        ('/home/csv/bee-0602.csv', r'/home/neo4j-1/import/bee.csv', BEE_header, 'BEE', '人员任职'),
        ('/home/csv/spe-0602.csv', r'/home/neo4j-1/import/spe.csv', SPE_header, 'SPE', '专利节点'),

        ('/home/csv/opep-0602.csv', r'/home/neo4j-1/import/opep.csv', OPEP_header, 'OPEP', '专利关系'),
        ('/home/csv/pp-0602.csv', r'/home/neo4j-1/import/pp.csv', PP_header, 'PP', '专利关系'),

        ('/home/csv/lel-0602.csv', r'/home/neo4j-1/import/lel.csv', LEL_header, 'LEL', '诉讼关系'),
        ('/home/csv/ll-0602.csv', r'/home/neo4j-1/import/ll.csv', LL_header, 'LL', '诉讼节点'),

        ('/home/csv/web-0602.csv', r'/home/neo4j-1/import/web.csv', WEB_header, 'WEB', '招投标关系'),
        ('/home/csv/gb-0602.csv', r'/home/neo4j-1/import/gb.csv', GB_header, 'GB', '招投标节点'),

        ('/home/csv/red-0602.csv', r'/home/neo4j-1/import/red.csv', RED_header, 'RED', '相同办公地'),
        ('/home/csv/dd-0602.csv', r'/home/neo4j-1/import/dd.csv', DD_header, 'DD', '办公地节点'),

        ('/home/csv/leet-0602.csv', r'/home/neo4j-1/import/leet.csv', LEET_header, 'LEET', '相同联系方式'),
        ('/home/csv/tt-0602.csv', r'/home/neo4j-1/import/tt.csv', TT_header, 'TT', '电话节点'),

        ('/home/csv/leee-0602.csv', r'/home/neo4j-1/import/leee.csv', LEEE_header, 'LEEE', '相同联系方式'),
        ('/home/csv/ee-0602.csv', r'/home/neo4j-1/import/ee.csv', EE_header, 'EE', '邮箱节点'),
    ]

    p = Pool(cpu_count() - 1)
    for read, write, header, label, desc in files:
        p.apply_async(func=task, args=(read, write, header, label, desc, EtlMigrate))

    p.close()
    p.join()

    # 删除原有的数据库，导入新的数据库
    rm_cmd = f'rm -rf /home/neo4j-1/data/databases/graph.db/*'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)

    import_cmd = "docker exec -it neo4j-1 /bin/bash -c 'bin/neo4j-admin import " \
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
                 "--ignore-missing-nodes --ignore-duplicate-nodes --high-io=true --multiline-fields=true'"
    os.system(import_cmd)
    os.system('docker restart neo4j-1')

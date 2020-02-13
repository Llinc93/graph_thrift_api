# coding: utf8
import os
import subprocess
import csv
import cx_Oracle
import datetime


class Orcale2Neo4j(object):

    username = "ts_neo"
    userpwd = "T471ONd958"
    host = "172.27.2.3"
    port = 1521
    dbname = "ocrl"

    # username = 'system'
    # userpwd = '123456'
    # host = "127.0.0.1"
    # port = 1521
    # dbname = 'dmp'

    # CSV配置
    ent_node_header = ['APPRDATE', 'CANDATE', 'DISTRICT', 'DOM', 'ENDDATE', 'NAME', 'NAME_GLLZD', 'ENTSTATUS', 'ENTTYPE', 'ESDATE', 'INDUSTRY', 'PERSONNAME', 'OPFROM', 'OPSCOPE', 'OPTO', 'REGCAP', 'RECCAPCUR', 'REGNO', 'REGORG', 'REVDATE', 'PROVINCE', 'UNISCID', 'ID:ID(ENT-ID)', ':LABEL']
    person_node_header = ['NAME', 'NAME_GLLZD', 'ID:ID(P-ID)', ':LABEL']

    inv_relationship_header = ['ACCONAM', 'BLICNO', 'BLICTYPE', 'CONDATE', 'ID::START_ID(P-ID)', 'INVTYPE', 'PROVINCE', 'PROVINCE_INV', 'SUBCONAM', 'RATE', 'F_BATCH', 'ID:END_ID(ENT-ID)', ':TYPE']
    ent_inv_relationship_header = ['ACCONAM', 'BLICNO', 'BLICTYPE', 'CONDATE', 'ID::START_ID(ENT-ID)', 'INVTYPE', 'PROVINCE', 'PROVINCE_INV', 'SUBCONAM', 'RATE', 'F_BATCH', 'ID:END_ID(ENT-ID)', ':TYPE']
    bra_relationship_header = ['UDT', 'ID:END_ID(ENT-ID)', 'B_NODENUM', 'ID', 'IDT', 'ID:START_ID(ENT-ID)', 'P_NODENUM', ':TYPE']

    csv_path = r'/opt/neo4j/import'

    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        dsn = cx_Oracle.makedsn(self.host, self.port, self.dbname)
        self.conn = cx_Oracle.connect(self.username, self.userpwd, dsn, encoding='utf8', nencoding='utf8')
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()
        return None

    def write_csv(self, file_type, ret):
        with open(os.path.join(self.csv_path, f'{file_type}.csv'), "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerows([getattr(self, f"{file_type}_header")])
            writer.writerows(ret)

    def get_ent(self):
        '''获取企业节点'''

        count_sql = 'select count(*) from TSSJJH.ZJ_ENTERPRISEBASEINFOCOLLECT'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct APPRDATE, CANDATE, DISTRICT, DOM, ENDDATE, ENTNAME, ENTNAME_GLLZD, ' \
              'ENTSTATUS, ENTTYPE, ESDATE, INDUSTRY, NAME, OPFROM, OPSCOPE, OPTO, REGCAP, RECCAPCUR, REGNO, ' \
              'REGORG, REVDATE, PROVINCE, UNISCID, F_BATCH, LCID from TSSJJH.ZJ_ENTERPRISEBASEINFOCOLLECT ' \
              'where rownum < %s  minus  select distinct APPRDATE, CANDATE, DISTRICT, DOM, ENDDATE, ' \
              'ENTNAME, ENTNAME_GLLZD, ENTSTATUS, ENTTYPE, ESDATE, INDUSTRY, NAME, OPFROM, OPSCOPE, OPTO, ' \
              'REGCAP, RECCAPCUR, REGNO, REGORG, REVDATE, PROVINCE, UNISCID, F_BATCH, LCID from ' \
              'TSSJJH.ZJ_ENTERPRISEBASEINFOCOLLECT where rownum < %s  order by LCID'
        data = []
        pos = 1
        for index in range(500001, count + 2, 500000):
            self.cursor.execute(sql % (index, pos))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
            pos = index
        else:
            self.cursor.execute(sql % (pos, count + 2))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)

        # sql = 'select distinct APPRDATE, CANDATE, DISTRICT, DOM, ENDDATE, ENTNAME, ENTNAME_GLLZD, ENTSTATUS, ENTTYPE, ESDATE, INDUSTRY, NAME, OPFROM, OPSCOPE, OPTO, REGCAP, RECCAPCUR, REGNO, REGORG, REVDATE, PROVINCE, UNISCID, F_BATCH, LCID from TSSJJH.ZJ_ENTERPRISEBASEINFOCOLLECT'
        # self.cursor.execute(sql)
        # ret = self.cursor.fetchall()
        # print([i[0] for i in self.cursor.description])
        #
        # data = []
        # for i in ret:
        #     tmp = [k if k else 'null' for k in i]
        #     tmp.append('GS')
        #     data.append(tmp)
        #     index[i[-1]] = i[-2]

        self.write_csv('ent_node', data)
        return data

    def get_person(self):
        '''获取人员节点'''

        count_sql = 'select count(*) from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct INVNAME, INVNAME_GLLZD, PID_INV from TSSJJH.ZJ_E_INV_INVESTMENT_BT ' \
              'where PID_INV is not null and rownum < %s  minus  select distinct INVNAME, INVNAME_GLLZD, ' \
              'PID_INV from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null and rownum < %s  order by PID_INV'
        data = []
        pos = 1
        for index in range(500001, count + 2, 500000):
            self.cursor.execute(sql % (index, pos))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
            pos = index
        else:
            self.cursor.execute(sql % (pos, count + 2))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)

        # sql = 'select distinct INVNAME, INVNAME_GLLZD, PID_INV from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null'
        # self.cursor.execute(sql)
        # ret = self.cursor.fetchall()
        # print([i[0] for i in self.cursor.description])
        # print(len(ret))
        # data = []
        # for k in ret:
        #     tmp = [l if l else 'null' for l in k]
        #     tmp.append('GR')
        #     data.append(tmp)

        self.write_csv('person_node', data)
        return data

    def get_inv_relationship(self):
        '''获取投资关系'''

        count_sql = 'select count(*) from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null and LCID_INV is null'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, PROVINCE_INV, ' \
              'SUBCONAM, RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT ' \
              'where PID_INV is not null and LCID_INV is null and rownum < %s  minus  ' \
              'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, ' \
              'PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null and ' \
              'LCID_INV is null and rownum < %s  order by PID_INV'
        data = []
        pos = 1
        for index in range(500001, count + 2, 500000):
            self.cursor.execute(sql % (index, pos))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
            pos = index
        else:
            self.cursor.execute(sql % (pos, count + 2))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
        self.write_csv('inv_relationship', data)

        count_sql2 = 'select count(*) from TSSJJH.ZJ_E_INV_INVESTMENT_BT where LCID_INV is not null and PID_INV is null'
        self.cursor.execute(count_sql2)
        count2 = int(self.cursor.fetchall()[0][0])

        sql2 = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, LCID_INV, INVTYPE, PROVINCE, PROVINCE_INV, ' \
               'SUBCONAM, RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT ' \
               'where LCID_INV is not null and PID_INV is null and rownum < %s  minus  select distinct ' \
               'ACCONAM, BLICNO, BLICTYPE, CONDATE, LCID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, ' \
               'RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT where LCID_INV is not null and ' \
               'PID_INV is null and rownum < %s  order by PID_INV'
        pos = 1
        for index in range(500001, count2 + 2, 500000):
            self.cursor.execute(sql2 % (index, pos))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
            pos = index
        else:
            self.cursor.execute(sql2 % (pos, count + 2))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
        self.write_csv('ent_inv_relationship', data)

        # sql = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null and LCID_INV is null'
        # self.cursor.execute(sql)
        # ret = self.cursor.fetchall()
        # print([i[0] for i in self.cursor.description])
        # data = []
        # for i in ret:
        #     tmp = [k if k else 'null' for k in i]
        #     tmp.append('IPEE')
        #     data.append(tmp)
        #
        # sql2 = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, LCID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT where LCID_INV is not null and PID_INV is null'
        # self.cursor.execute(sql2)
        # ret2 = self.cursor.fetchall()
        # for m in ret2:
        #     tmp = [n if n else 'null' for n in m]
        #     tmp.append('IPEE')
        #     data.append(tmp)


        return data

    def get_bra_relationship(self):
        '''获取企业分支关系'''
        count_sql = 'select count(*) from TSSJJH.ZJ_BRANCH'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        data = []
        pos = 1
        sql = 'select distinct UDT, B_LCID, B_NODENUM, ID, IDT, P_LCID, P_NODENUM from TSSJJH.ZJ_BRANCH ' \
              'where rownum < %s  minus  select distinct UDT, B_LCID, B_NODENUM, ID, IDT, P_LCID, P_NODENUM ' \
              'from TSSJJH.ZJ_BRANCH where rownum < %s  order by B_LCID, P_LCID'
        for index in range(500001, count + 2, 500000):
            self.cursor.execute(sql % (index, pos))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)
            pos = index
        else:
            self.cursor.execute(sql % (pos, count + 2))
            for i in self.cursor.fetchall():
                tmp = [k if k else 'null' for k in i]
                tmp.append('BEE')
                data.append(tmp)

        # sql = 'select distinct UDT, B_LCID, B_NODENUM, ID, IDT, P_LCID, P_NODENUM from TSSJJH.ZJ_BRANCH'
        # self.cursor.execute(sql)
        # ret = self.cursor.fetchall()
        # print([i[0] for i in self.cursor.description])
        # data = []
        # for i in ret:
        #     tmp = [k if k else 'null' for k in i]
        #     tmp.append('BEE')
        #     data.append(tmp)

        self.write_csv('bra_relationship', data)
        return data

    def run(self):
        # 企业节点
        # ret = self.get_ent()
        # print(len(ret))

        # 投资人员节点
        # ret = self.get_person()
        # print(len(ret))

        # 投资关系
        # ret = self.get_inv_relationship()
        # print(len(ret))

        # 分支关系
        ret = self.get_bra_relationship()
        # print(len(ret))


if __name__ == '__main__':
    # 查询数据库，生成CSV文件
    with Orcale2Neo4j() as conn:
        conn.run()

    # 将csv文件导入neoj
    rm_cmd = 'rm -rf /opt/neo4j/data/databases/graph.db'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)
    print(rm_ret)
    import_cmd = "docker exec -it neo4j_dmp /bin/bash -c 'bin/neo4j-admin import --nodes=import/person_node.csv --nodes=import/ent_node.csv --relationships=import/inv_relationship.csv --relationships=import/bra_relationship.csv --ignore-missing-nodes --ignore-duplicate-nodes'"
    import_code, import_ret = subprocess.getstatusoutput(import_cmd)
    print(import_ret)
    os.system('chown -R 101:100 /opt/neo4j/data/databases/graph.db')
    os.system('docker restart neo4j_graph')

    # neo4j 数据库创建索引
    'create index on :GS(NAME)'



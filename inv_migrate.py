# coding: utf8
import os
import subprocess
import csv
import cx_Oracle
import datetime
import time
from py2neo import Graph

import config

'''
sql = 'select a.* from (select t.*, rownum row from table_A t where rownum <= 20) a where a.row > 0'
'''

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
        self.ent_node_count = 0
        self.person_node_count = 0
        self.inv_ent_ent = 0
        self.inv_per_ent = 0

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
        '''获取企业节点(66736199)'''

        count_sql = 'select count(*) from ENTERPRISEBASEINFOCOLLECT'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct a.APPRDATE, a.CANDATE, a.DISTRICT, a.DOM, a.ENDDATE, a.ENTNAME, a.ENTNAME_GLLZD, ' \
              'a.ENTSTATUS, a.ENTTYPE, a.ESDATE, a.INDUSTRY, a.NAME, a.OPFROM, a.OPSCOPE, a.OPTO, a.REGCAP, a.RECCAPCUR, a.REGNO, ' \
              'a.REGORG, a.REVDATE, a.PROVINCE, a.UNISCID, a.LCID from (select t.*, rownum rc from ENTERPRISEBASEINFOCOLLECT t ' \
              'where rownum <= %s) a where a.rc > %s'

        data = []
        pos = 0
        for index in range(200000, count + 2, 200000):
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

        self.write_csv('ent_node', data)
        self.ent_node_count = len(data)
        return data

    def get_person(self):
        '''获取人员节点(120320809)'''

        count_sql = 'select count(*) from E_INV_INVESTMENT where PID_INV is not null'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct a.INVNAME, a.INVNAME_GLLZD, a.PID_INV from (select t.*, rownum rc ' \
              'from E_INV_INVESTMENT t where rownum <= %s) a where a.rc > %s'

        data = []
        pos = 0
        for index in range(200000, count + 2, 200000):
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

        self.write_csv('person_node', data)
        self.person_node_count = len(data)
        return data

    def get_inv_relationship(self):
        '''获取投资关系(120320809)'''

        count_sql = 'select count(*) from E_INV_INVESTMENT_TS where PID_INV is not null and LCID_INV is null'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        sql = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, ' \
              'PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID from (select t.*, rownum rc ' \
              'from E_INV_INVESTMENT_TS t where t.PID_INV is not null and t.LCID_INV is null and rownum <= %s) a ' \
              'where a.PID_INV is not null and a.LCID_INV is null and a.rc > %s'

        data = []
        pos = 0
        for index in range(200000, count + 2, 200000):
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
        self.inv_per_ent = len(data)
        count_sql2 = 'select count(*) from TSSJJH.E_INV_INVESTMENT where LCID_INV is not null and PID_INV is null'
        self.cursor.execute(count_sql2)
        count2 = int(self.cursor.fetchall()[0][0])

        sql2 = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, ' \
              'PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID from (select t.*, rownum rc ' \
              'from E_INV_INVESTMENT t where t.LCID_INV is not null and t.PID_INV is null and rownum <= %s) a ' \
              'where a.LCID_INV is not null and a.PID_INV is null and a.rc > %s'
        data = []
        pos = 0
        for index in range(200000, count2 + 2, 200000):
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
        self.inv_ent_ent = len(data)
        return data

    def get_bra_relationship(self):
        '''获取企业分支关系(7665150)'''
        count_sql = 'select count(*) from TSSJJH.BRANCH'
        self.cursor.execute(count_sql)
        count = int(self.cursor.fetchall()[0][0])

        data = []
        pos = 0
        sql = 'select distinct UDT, B_LCID, B_NODENUM, ID, IDT, P_LCID, P_NODENUM from (select t.*, rownum rc from F_ENTBRANCH_TS t where t.row <= %s) a where a.rc > %s'

        for index in range(200000, count + 2, 100000):
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

        self.write_csv('bra_relationship', data)
        return data

    def run(self):
        # 企业节点
        ret = self.get_ent()
        # print(len(ret))

        # 投资人员节点
        ret = self.get_person()
        # print(len(ret))

        # 投资关系
        ret = self.get_inv_relationship()
        # print(len(ret))

        # 分支关系
        ret = self.get_bra_relationship()
        # print(len(ret))
        return None

    def create_index(self):
        command = 'create index on :GS(NAME)'
        self.graph = Graph(config.NEO4J_URL)
        self.graph.run(command)
        return None

if __name__ == '__main__':
    # 查询数据库，生成CSV文件
    with Orcale2Neo4j() as conn:
        conn.run()
        ent_node = conn.ent_node_count
        per_node = conn.person_node_count
        inv_ent_ent = conn.inv_ent_ent
        inv_per_ent = conn.inv_per_ent

    # 将csv文件导入neoj
    rm_cmd = f'mv /opt/neo4j/data/databases/graph.db /opt/neo4j/data/databases/graph.db_{time.strftime("%Y%m%d")}'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)
    print(rm_ret)
    import_cmd = "docker exec -it neo4j_dmp /bin/bash -c 'bin/neo4j-admin import --nodes=import/person_node.csv --nodes=import/ent_node.csv --relationships=import/inv_relationship.csv --relationships=import/bra_relationship.csv --ignore-missing-nodes --ignore-duplicate-nodes'"
    import_code, import_ret = subprocess.getstatusoutput(import_cmd)
    print(import_ret)
    os.system('chown -R 101:100 /opt/neo4j/data/databases/graph.db')
    os.system('docker restart neo4j_graph')

    # neo4j 数据库创建索引
    Orcale2Neo4j().create_index()
    os.system('docker restart neo4j_graph')


# coding: utf8
import os
import subprocess
import csv
import cx_Oracle
import copy
import datetime
import hashlib


'''
sql = "select * from DT_BASE_DTLS"

cursor.execute(sql)

result = cursor.fetchall()

count = cursor.rowcount

for  row  in  result: 

 print (row) 

cursor.close() 

connection.close() 
'''


class Orcale2Neo4j(object):

    username = "system"
    userpwd = "123456"
    host = "localhost"
    port = 1521
    dbname = "dmp2neo"

    # CSV配置
    ent_node_header = ['APPRDATE', 'CANDATE', 'DISTRICT', 'DOM', 'ENDDATE', 'NAME', 'NAME_GLLZD', 'ENTSTATUS', 'ENTTYPE', 'ESDATE', 'INDUSTRY', 'PERSONNAME', 'OPFROM', 'OPSCOPE', 'OPTO', 'REGCAP', 'RECCAPCUR', 'REGNO', 'REGORG', 'REVDATE', 'PROVINCE', 'UNISCID', 'F_BATCH', 'ID:ID(ENT-ID)', 'ROWKEY', ':LABEL']
    person_node_header = ['NAME', 'NAME_GLLZD', 'ID:ID(P-ID)', ':LABEL']

    inv_relationship_header = ['ACCONAM', 'BLICNO', 'BLICTYPE', 'CONDATE', 'ID::START_ID(P-ID)', 'INVTYPE', 'PROVINCE', 'PROVINCE_INV', 'SUBCONAM', 'RATE', 'F_BATCH', 'ID:END_ID(ENT-ID)', 'ID', ':TYPE']
    other_relationship_header = ['ACCONAM', 'BLICNO', 'BLICTYPE', 'CONDATE', 'ID::START_ID(P-ID)', 'INVTYPE', 'PROVINCE', 'PROVINCE_INV', 'SUBCONAM', 'RATE', 'F_BATCH', 'ID:END_ID(ENT-ID)', 'ID', ':TYPE']
    ent_inv_relationship_header = ['ACCONAM', 'BLICNO', 'BLICTYPE', 'CONDATE', 'ID::START_ID(ENT-ID)', 'INVTYPE', 'PROVINCE', 'PROVINCE_INV', 'SUBCONAM', 'RATE', 'F_BATCH', 'ID:END_ID(ENT-ID)', 'ID', ':TYPE']
    pos_relationship_header = ['LEREPSIGN', 'ID::START_ID(P-ID)', 'POSITION', 'PROVINCE', 'ID:END_ID(ENT-ID)', 'F_BATCH', 'ID', ':TYPE']
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
        # with open(r"C:\Users\cpf\Desktop\{}.csv".format(file_type), "w", encoding="utf-8", newline="") as fp:
        with open(os.path.join(self.csv_path, f'{file_type}.csv'), "w", encoding="utf-8", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerows([getattr(self, f"{file_type}_header")])
            writer.writerows(ret)

    def get_ent(self):
        '''获取企业节点'''
        sql = 'select distinct * from TSSJJH.ZJ_ENTERPRISEBASEINFOCOLLECT'
        self.cursor.execute(sql)
        ret = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        data = []
        index = set()
        for i in ret:
            tmp = [k if k else 'null' for k in i]
            if i[-2] in index:
                continue
            # tmp = list(i)
            tmp.append('GS')
            data.append(tmp)
            index.add(i[-2])

        self.write_csv('ent_node', data)
        return data

    def get_md5(self, item):
        return hashlib.md5(','.join(item).encode('utf8')).hexdigest()

    def get_person(self):
        '''获取人员节点'''
        # sql = 'select distinct a.INVNAME, a.INVNAME_GLLZD, a.ENTNAME from TSSJJH.ZJ_E_INV_INVESTMENT_BT a inner join TSSJJH.ZJ_E_PRI_PERSON  b on a.LCID = b.LCID and a.INVNAME=b.NAME'
        sql1 = 'select distinct NAME, NAME_GLLZD, PID from TSSJJH.ZJ_E_PRI_PERSON where PID is not null'
        self.cursor.execute(sql1)
        ret = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        print(len(ret))
        data = []
        pid_set = set()
        for i in ret:
            tmp = [k if k else 'null' for k in i]
            # tmp = list(i)
            tmp.append('GR')
            data.append(tmp)
            pid_set.add(i[-1])

        sql2 = 'select distinct INVNAME, INVNAME_GLLZD, PID_INV from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null'
        self.cursor.execute(sql2)
        ret2 = self.cursor.fetchall()
        print(len(ret2))
        for m in ret2:
            tmp = [n if n else 'null' for n in m]
            if m[-1] in pid_set:
                continue
            # tmp = list(k)
            tmp.append('GR')
            data.append(tmp)
            pid_set.add(m[-1])

        sql3 = 'select distinct inv.INVNAME, inv.INVNAME_GLLZD, inv.LCID from TSSJJH.ZJ_E_INV_INVESTMENT_BT inv INNER JOIN TSSJJH.ZJ_E_PRI_PERSON pos on inv.LCID=pos.LCID and pos.name=inv.INVNAME where inv.LCID_INV is null and inv.PID_INV is null'
        self.cursor.execute(sql3)
        ret3 = self.cursor.fetchall()
        print(len(ret3))
        for k in ret3:
            tmp = [l if l else 'null' for l in k]
            if k[-1] in pid_set:
                continue
            # tmp = list(k)
            tmp[-1] = self.get_md5([tmp[0], tmp[-1]])
            tmp.append('GR')
            data.append(tmp)
            pid_set.add(tmp[-2])
        self.write_csv('person_node', data)
        return data

    def get_pos_relationship(self):
        '''获取任职关系'''
        sql = "select distinct LEREPSIGN, PID, POSITION, PROVINCE, LCID, F_BATCH, ROWKEY from TSSJJH.ZJ_E_PRI_PERSON where PID is not null"  # 434814
        self.cursor.execute(sql)
        ret = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        data = []
        pid_set = set()
        for i in ret:
            tmp = [k if k else 'null' for k in i]
            # tmp = list(i)
            tmp.append('SPE')
            data.append(tmp)
            pid_set.add(i[1])

        sql2 = "select distinct LEREPSIGN, PID, POSITION, PROVINCE, LCID, F_BATCH, ROWKEY, NAME from TSSJJH.ZJ_E_PRI_PERSON where PID is null"
        self.cursor.execute(sql2)
        ret2 = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        data = []
        for i in ret2:
            if i[-1] in pid_set:
                continue
            tmp = [k if k else 'null' for k in i]
            name = tmp.pop()
            # tmp = list(i)
            tmp.append('SPE')
            tmp[1] = self.get_md5([name, tmp[4]])
            data.append(tmp)

        self.write_csv('pos_relationship', data)
        return data

    def get_inv_relationship(self):
        '''获取投资关系'''
        sql = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID, ROWKEY from TSSJJH.ZJ_E_INV_INVESTMENT_BT where PID_INV is not null'  # 176726
        self.cursor.execute(sql)
        ret = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        data = []
        for i in ret:
            tmp = [k if k else 'null' for k in i]
            # tmp = list(i)
            tmp.append('IPEE')
            data.append(tmp)
        self.write_csv('inv_relationship', data)

        sql2 = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, LCID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID, ROWKEY from TSSJJH.ZJ_E_INV_INVESTMENT_BT where LCID_INV is not null'  # 176726
        self.cursor.execute(sql2)
        ret2 = self.cursor.fetchall()
        data = []
        for m in ret2:
            tmp = [n if n else 'null' for n in m]
            # tmp = list(i)
            tmp.append('IPEE')
            data.append(tmp)

        sql3 = 'select distinct ACCONAM, BLICNO, BLICTYPE, CONDATE, PID_INV, INVTYPE, PROVINCE, PROVINCE_INV, SUBCONAM, RATE, F_BATCH, LCID, ROWKEY, INVNAME from TSSJJH.ZJ_E_INV_INVESTMENT_BT where LCID_INV is null and PID_INV is null'  # 176726
        self.cursor.execute(sql3)
        ret3 = self.cursor.fetchall()
        data = []
        for m in ret3:
            tmp = [n if n else 'null' for n in m]
            name = tmp.pop()
            # tmp = list(i)
            tmp[4] = self.get_md5([name, tmp[11]])
            tmp.append('IPEE')
            data.append(tmp)
        self.write_csv('other_relationship', data)

        return ret

    def get_bra_relationship(self):
        '''获取企业分支关系'''
        sql = 'select distinct UDT, B_LCID, B_NODENUM, ID, IDT, P_LCID, P_NODENUM from TSSJJH.ZJ_BRANCH'
        self.cursor.execute(sql)
        ret = self.cursor.fetchall()
        print([i[0] for i in self.cursor.description])
        data = []
        for i in ret:
            tmp = [k if k else 'null' for k in i]
            # tmp = list(i)
            tmp.append('BEE')
            data.append(tmp)

        self.write_csv('bra_relationship', data)
        return ret

    def run(self):
        # 企业节点
        ret = self.get_ent()
        # print(len(ret))
        # for i in ret:
        #     print(i)
        # 投资人员节点
        ret = self.get_person()
        # print(len(ret))

        # 投资关系
        ret = self.get_inv_relationship()
        # print(len(ret))

        # 任职关系
        ret = self.get_pos_relationship()
        # print(len(ret))

        # 分支关系
        ret = self.get_bra_relationship()
        # print(len(ret))


if __name__ == '__main__':
    # 将dmp文件导入Orcale

    # 生成CSV文件
    with Orcale2Neo4j() as conn:
        conn.run()

    # 将csv文件导入neoj
    rm_cmd = 'rm -rf /opt/neo4j/data/databases/graph.db'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)
    print(rm_ret)
    import_cmd = "docker exec -it neo4j_dmp /bin/bash -c 'bin/neo4j-admin import --nodes=import/person_node.csv --nodes=import/ent_node.csv --relationships=import/inv_relationship.csv --relationships=import/ent_inv_relationship.csv  --relationships=import/pos_relationship.csv --relationships=import/bra_relationship.csv --ignore-missing-nodes --ignore-duplicate-nodes'"
    import_code, import_ret = subprocess.getstatusoutput(import_cmd)
    print(import_ret)
    os.system('chown -R 101:100 /opt/neo4j/data/databases/graph.db')
    os.system('docker restart neo4j_graph')


    #todo
    '''
    企业曾用名表   F_ENT_NAME_HISTORY_TABLE
        历史名称关系
        合并到企业节点
     
    招投标公司表    F_ZFCG_ENT
        招投标关系
        使用企业名称关联
        
    专利权表        F_SQZLQR
        专利关系
        找不到关联项
        
    法律文书公司表     F_FLWS_ENT
        企业名称关联
    
    法律文书信息表
        通过案号与法律文书公司表的案号关联
    
    年报-企业基本信息表
        通过企业名称关联
        
    
    企业名称关联
    人员名称关联    
    
    节点：
        企业节点  企业信息表
        人员节点  任职表、投资表
        办公地、邮箱、电话  企业信息表抽取
        
        法律信息、法律公司 通过案号合一，作为诉讼节点
        
        专利节点 ？？？
        
        招投标节点 --- 招投标表
    
    关系
        投资关系 --- 投资表
        任职关系 --- 任职表
        分支关系 --- 分支表
        招投标关系 --- 招投标表
        相同办公地 --- 企业表
        相同联系方式 --- 企业表 ??? 电话 or 邮箱
        专利关系 --- 专利表
        诉讼关系 --- 法律信息、法律公司
        历史投资关系 ？？？
        历史任职关系 ？？？
    neo4j 
        企业节点、人员节点、招投标节点、办公地点节点、邮箱节点、电话节点、专利节点、诉讼节点
        投资关系、任职关系、分支机构、招投标关系、相同办公地、相同联系方式、共有专利关系、诉讼关系、历史投资关系、历史任职关系
    '''


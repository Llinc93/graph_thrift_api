# coding: utf8
import os
import subprocess
import csv


'''
select INVNAME, PID_INV from E_INV_INVESTMENT where PID_INV is not null
select ENTNAME, UNISCID, LCID from ENTERPRISEBASEINFOCOLLECT
select PID_INV, RATE, LCID from E_INV_INVESTMENT where PID_INV is not null and LCID_INV is null
select LCID_INV, RATE, LCID from E_INV_INVESTMENT where PID_INV is null and LCID_INV is not null
select B_LCID, P_LCID from F_ENTBRANCH_TS
'''

ent_node_header = ['NAME', 'UNISCID', 'ID:ID(ENT-ID)', ':LABEL']
person_node_header = ['NAME', 'ID:ID(P-ID)', ':LABEL']

inv_relationship_header = ['ID:START_ID(P-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
ent_inv_relationship_header = ['ID:START_ID(ENT-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
bra_relationship_header = ['ID:END_ID(ENT-ID)', 'ID:START_ID(ENT-ID)', ':TYPE']

report = {}
csv_path = r'/opt/neo4j_v2/import'

# CS文件位置、导入用的neo4j的CSV文件位置(无需修改)、CSV文件字段定义、关系类型、说明
files = [
    ('/opt/data/E_INV_INVESTMENT_ENT_20200215.csv', '/opt/neo4j_v2/import/ent_inv_relationship.csv', ent_inv_relationship_header, 'IPEE', '企业投资关系'),
    ('/opt/data/E_INV_INVESTMENT_ENT_PERSION', '/opt/neo4j_v2/import/inv_relationship.csv', inv_relationship_header, 'IPEE', '股东投资关系'),
    ('/opt/data/enterprisebaseinfocollect.csv', '/opt/neo4j_v2/import/ent_node.csv', ent_node_header, 'GS', '企业节点'),
    ('/opt/data/F_ENTBRRANCH_TS.csv', '/opt/neo4j_v2/import/bra_relationship.csv', bra_relationship_header, 'BEE', '企业分支关系'),
    ('/opt/data/persion.csv', '/opt/neo4j_v2/import/person_node.csv', person_node_header, 'GR', '人员节点')
]

for read_file, write_file, header, label, desc in files:
    rf = open(read_file, 'r', encoding='utf8')
    wf = open(write_file, 'w', encoding='utf8')

    index = 1
    for row in csv.reader(rf):
        writer = csv.writer(wf)
        if index == 1:
            writer.writerow(header)
        else:
            row.append(label)
            writer.writerow([k if k else 'null' for k in row])
        index += 1
    else:
        report[desc] = index

    rf.close()
    wf.close()


if __name__ == '__main__':

    # 将csv文件导入neoj
    rm_cmd = f'rm -rf /opt/neo4j_graph/data/graph.db'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)
    import_cmd = "docker exec -it neo4j_graph /bin/bash -c 'bin/neo4j-admin import --nodes=import/person_node.csv --nodes=import/ent_node.csv --relationships=import/inv_relationship.csv --relationships=import/ent_inv_relationship.csv --relationships=import/bra_relationship.csv --ignore-missing-nodes --ignore-duplicate-nodes'"
    os.system(import_cmd)
    os.system('docker restart neo4j_graph')


    # 返回表中数据统计
    for key, value in report.items():
        print(key, value)


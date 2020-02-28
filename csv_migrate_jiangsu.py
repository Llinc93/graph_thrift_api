# coding: utf8
import os
import hashlib
import subprocess
import csv

'''
企业节点.csv   --- 企业基本信息表
LCID、ENTNAME、UNISCID

人员节点.csv  --- 企业股东表和主要管理人员表
PID、NAME

自然人投资.csv --- 企业股东表
PID_INV、RATE、LCID

企业投资.csv  --- 企业股东表
LCID_INV、RATE、LCID

分支.csv   --- 企业分支表
B_LCID、P_LCID

任职.csv   --- 主要管理人员表
LCID、POSITION、PID
'''

report = {}
# csv_path = r'/opt/neo4j_v2/import'

# CS文件位置、导入用的neo4j的CSV文件位置(无需修改)、CSV文件字段定义、关系类型、说明

ent_node_header = ['ID:ID(ENT-ID)', 'NAME', 'UNISCID', ':LABEL']
person_node_header = ['ID:ID(P-ID)', 'NAME', ':LABEL']

inv_relationship_header = ['ID:START_ID(P-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
ent_inv_relationship_header = ['ID:START_ID(ENT-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
bra_relationship_header = ['ID:END_ID(ENT-ID)', 'ID:START_ID(ENT-ID)', ':TYPE']
pos_relationship_header = ['ID:START_ID(P-ID)', 'POSITION', 'ID:END_ID(ENT-ID)', ':TYPE']

'''
FZL_MC,FZL_SQH,FZL_SQZLQR,FZL_STATUS,FZL_FMSJR,LCID
一种电梯导轨,CN201220001643.4,苏州塞维拉上吴电梯轨道系统有限公司,1,蔡连生;邹征;蔡斌斌;王四新,4d202bf24e13d784b5d05f8a38195cd5
近纳诱捕鲶鱼音波装置,CN201220324073.2,徐州一统渔具有限公司,1,尹克华,b7f87cf2854f481d88ab83c72343b688
'''
fzl_node_header = ['ID:ID(FZL-ID)', 'NAME', 'SQH', ':LABEL']
fzl_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(FZL-ID)', ':TYPE']

'''
FFL_TITLE,FFL_ENTNAME_GLLZD,LCID,FFL_CASENUM,FFL_STATUS
王祥子虚开增值税专用发票、用于骗取出口退税、抵扣税款发票一审刑事判决书,盐城市艳阳棉业有限公司,0b4bf97634753010e8e5e8cc36ead307,（2014）沪二中刑初字第48号,1
苏州建恒国际货运代理有限公司与王香不当得利纠纷一审民事判决书,苏州建恒国际货运代理有限公司,84dcb2ecf8276ed598651c7775c28584,（2014）虎民初字第1521号,1
'''
ffl_node_header = ['ID:ID(FFL-ID)', 'NAME', 'CASENUM', ':LABEL']
ffl_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(FFL-ID)', ':TYPE']

'''
FZE_TITLE,FZE_ENTNAME_GLLZD,FZE_ZBBH,FZE_STATUS,LCID
苏州市市容市政管理局作业车辆智能管理及终端视频监管项目中标公告,江苏移动信息系统集成有限公司,,1,0da905337f2ac64bdd4a069d74f50946
上海正弘建设工程顾问有限公司关于连通水系两岸（肖家村至西环路）环境整治工程中标候选人公示,江苏科晟园林景观建设集团有限公司,,1,408dd7b1135077a76169e56d0c112b0a
'''
fze_node_header = ['ID:ID(FZE-ID)', 'NAME', 'ZBBH', ':LABEL']
fze_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(FZE-ID)', ':TYPE']

'''
ADDR,ENTNAME_GLLZD,LCID
南京市秦淮区白鹭洲公园白鹭村1号,南京源承八九号文化艺术品投资合伙企业,1f063cd939edf1758b0542d850853a8f
洪泽县东风路84号,洪泽县四明眼镜有限公司,cb43f17129a7466593e1f1e76eea5a50
'''
addr_node_header = ['ID:ID(ADDR-ID)', 'NAME', ':LABEL']
addr_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(ADDR-ID)', ':TYPE']

'''
TEL,ENTNAME_GLLZD,LCID,EMAIL
025-83409842,南京源承八九号文化艺术品投资合伙企业,1f063cd939edf1758b0542d850853a8f,zxy@zixingyun.com
18021926600,洪泽县四明眼镜有限公司,cb43f17129a7466593e1f1e76eea5a50,511611378@qq.com
'''
tel_node_header = ['ID:ID(TEL-ID)', 'NAME', ':LABEL']
tel_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(TEL-ID)', ':TYPE']

email_node_header = ['ID:ID(EMAIL-ID)', 'NAME', ':LABEL']
email_relationship_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(EMAIL-ID)', ':TYPE']

files = [
    #('/opt/csv/企业投资.csv', r'/home/neo4j/import/ent_inv_relationship.csv', ent_inv_relationship_header, 'IPEE', '企业投资'),
    #('/opt/csv/自然人投资.csv', r'/home/neo4j/import/inv_relationship.csv', inv_relationship_header, 'IPEE', '股东投资'),
    #('/opt/csv/基本信息企业节点.csv', r'/home/neo4j/import/ent_node.csv', ent_node_header, 'GS', '企业节点'),
    #('/opt/csv/人员节点.csv', r'/home/neo4j/import/person_node.csv', person_node_header, 'GR', '人员节点'),
    #('/opt/csv/企业分支.csv', r'/home/neo4j/import/bra_relationship.csv', bra_relationship_header, 'BEE', '分支机构'),
    ('/opt/csv/任职.csv', r'/opt/neo4j/import/pos_relationship.csv', pos_relationship_header, 'SPE', '人员任职'),
    ('/opt/csv/专利_20200221.csv', r'/home/neo4j/import/fzl_node.csv', fzl_node_header, 'PP', '专利节点'),
    ('/opt/csv/专利_20200221.csv', r'/home/neo4j/import/fzl_relationship.csv', fzl_relationship_header, 'OPEP', '专利关系'),
    # ('/opt/csv/法律文书.csv', r'/home/neo4j/import/ffl_node.csv', ffl_node_header, 'LL', '诉讼节点'),
    # ('/opt/csv/法律文书.csv', r'/home/neo4j/import/ffl_relationship.csv', ffl_relationship_header, 'LEL', '诉讼关系'),
    # ('/opt/csv/招投标.csv', r'/home/neo4j/import/fze_node.csv', fze_node_header, 'GB', '招投标节点'),
    # ('/opt/csv/招投标.csv', r'/home/neo4j/import/fze_relationship.csv', fze_relationship_header, 'WEB', '招投标关系'),
    # ('/opt/csv/相同办公地_年报.csv', r'/home/neo4j/import/addr_node.csv', addr_node_header, 'DD', '办公地节点'),
    # ('/opt/csv/相同办公地_年报.csv', r'/home/neo4j/import/addr_relationship.csv', addr_relationship_header, 'WEB', '办公地关系'),
    # ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j/import/tel_node.csv', tel_node_header, 'TT', '电话节点'),
    # ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j/import/tel_relationship.csv', tel_relationship_header, 'LEE', '企业专利关系'),
    # ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j/import/email_node.csv', email_node_header, 'EE', '企业专利关系'),
    # ('/opt/csv/相同联系方式_年报.csv', r'/home/neo4j/import/email_relationship.csv', email_relationship_header, 'LEE', '企业专利关系'),
]


class WriteCSV(object):
    '''转化成neo4j需要的格式文件'''



    def get_id(self, name):
        return hashlib.md5(name.encode('utf8')).hexdigest()

    def GS(self, row):
        return row

    def GR(self, row):
        return row

    def GB(self, row):
        return [self.get_id(row[0]), row[0], row[2], 'GB']

    def DD(self, row):
        return [self.get_id(row[0]), row[0], 'DD']

    def EE(self, row):
        return [self.get_id(row[3]), row[3], 'EE']

    def TT(self, row):
        return [self.get_id(row[0]), row[0], 'TT']

    def PP(self, row):
        # return [self.get_id(row[0]), row[0], row[1], 'PP']
        return [row[0], row[0], row[1], 'PP']

    def LL(self, row):
        return [self.get_id(row[0]), row[0], row[3], 'LL']

    def IPEE(self, row):
        return [row[0], row[1] if row[1] else 0, row[2]]

    def SPE(self, row):
        return row

    def BEE(self, row):
        return row

    def WEB(self, row):
        return [row[2], self.get_id(row[0]), 'WEB']

    def RED(self, row):
        return [row[2], self.get_id(row[0]), 'RED']

    def LEE(self, row):
        return [row[2], self.get_id(row[0]), 'LEE'], [row[2], self.get_id(row[3]), 'LEE']

    def OPEP(self, row):
        # return [row[5], self.get_id(row[0]), 'OPEP']
        return [row[5], row[0], 'OPEP']

    def LEL(self, row):
        return [row[2], self.get_id(row[0]), 'LEL']


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
                    print(row)
                    continue
                if isinstance(new_row, tuple):
                    for item in new_row:
                        writer.writerow([k if k else 'null' for k in item])
                else:
                    writer.writerow([k if k else 'null' for k in new_row])
            index += 1
        else:
            report[desc] = index

        rf.close()
        wf.close()
    return None


if __name__ == '__main__':
    run()
    # 将csv文件导入neoj
    rm_cmd = f'rm -rf /opt/neo4j/data/databases/graph.db'
    rm_code, rm_ret = subprocess.getstatusoutput(rm_cmd)
    import_cmd = "docker exec -it neo4j_graph /bin/bash -c 'bin/neo4j-admin import " \
                 "--nodes=import/person_node.csv " \
                 "--nodes=import/ent_node.csv " \
                 "--nodes=import/fzl_node.csv " \
                 "--nodes=import/ffl_node.csv " \
                 "--nodes=import/fze_node.csv " \
                 "--nodes=import/addr_node.csv " \
                 "--nodes=import/tel_node.csv " \
                 "--nodes=import/email_node.csv " \
                 "--relationships=import/inv_relationship.csv " \
                 "--relationships=import/ent_inv_relationship.csv " \
                 "--relationships=import/bra_relationship.csv " \
                 "--relationships=import/pos_relationship.csv "\
                 "--relationships=import/fzl_relationship.csv " \
                 "--relationships=import/ffl_relationship.csv " \
                 "--relationships=import/fze_relationship.csv " \
                 "--relationships=import/addr_relationship.csv " \
                 "--relationships=import/tel_relationship.csv " \
                 "--relationships=import/email_relationship.csv " \
                 "--ignore-missing-nodes --ignore-duplicate-nodes --high-io=true'"
    os.system(import_cmd)
    os.system('docker restart neo4j_graph')


    # 返回表中数据统计
    for key, value in report.items():
        print(key, value)


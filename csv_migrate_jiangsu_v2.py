# coding: utf8
import os
import hashlib
import subprocess
import csv

'''
基本信息企业节点.csv   --- 企业基本信息表
LCID、ENTNAME、UNISCID、STATUS

人员节点.csv  --- 企业股东表和主要管理人员表
PID、NAME

自然人投资.csv --- 企业股东表
PID_INV、RATE、LCID

企业投资.csv  --- 企业股东表
LCID_INV、RATE、LCID

企业分支.csv   --- 企业分支表
B_LCID、P_LCID

任职.csv   --- 主要管理人员表
LCID、POSITION、PID


'''

report = {}

GS_header = ['ID:ID(ENT-ID)', 'NAME', 'UNISCID', 'ESDATE', 'INDUSTRY', 'PROVINCE', 'REGCAP', 'RECCAPCUR', 'ENTSTATUS', ':LABEL']
GR_header = ['ID:ID(P-ID)', 'NAME', ':LABEL']

IPEER_header = ['ID:START_ID(P-ID)', 'RATE', 'RATE_TYPE', 'ID:END_ID(ENT-ID)', ':TYPE']
IPEES_header = ['ID:START_ID(ENT-ID)', 'RATE', 'RATE_TYPE', 'ID:END_ID(ENT-ID)', ':TYPE']
BEE_header = ['ID:END_ID(ENT-ID)', 'ID:START_ID(ENT-ID)', ':TYPE']
SPE_header = ['ID:END_ID(ENT-ID)', 'POSITION', 'ID:START_ID(P-ID)', ':TYPE']

'''
FZL_MC,FZL_SQH,FZL_SQZLQR,FZL_STATUS,FZL_FMSJR,LCID
一种电梯导轨,CN201220001643.4,苏州塞维拉上吴电梯轨道系统有限公司,1,蔡连生;邹征;蔡斌斌;王四新,4d202bf24e13d784b5d05f8a38195cd5
近纳诱捕鲶鱼音波装置,CN201220324073.2,徐州一统渔具有限公司,1,尹克华,b7f87cf2854f481d88ab83c72343b688
'''
PP_header = ['ID:ID(FZL-ID)', 'NAME', 'FZL_SQH', ':LABEL']
OPEP_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(FZL-ID)', ':TYPE']

'''
FFL_TITLE,LCID,FFL_CASENUM,FFL_STATUS
王祥子虚开增值税专用发票、用于骗取出口退税、抵扣税款发票一审刑事判决书,盐城市艳阳棉业有限公司,0b4bf97634753010e8e5e8cc36ead307,（2014）沪二中刑初字第48号,1
苏州建恒国际货运代理有限公司与王香不当得利纠纷一审民事判决书,苏州建恒国际货运代理有限公司,84dcb2ecf8276ed598651c7775c28584,（2014）虎民初字第1521号,1
'''
LL_header = ['ID:ID(FFL-ID)', 'NAME', ':LABEL']
LEL_header = ['ID:END_ID(FFL-ID)', 'ID:START_ID(ENT-ID)', ':TYPE']

'''
FZE_TITLE,FZE_ENTNAME_GLLZD,FZE_ZBBH,FZE_STATUS,LCID
苏州市市容市政管理局作业车辆智能管理及终端视频监管项目中标公告,江苏移动信息系统集成有限公司,,1,0da905337f2ac64bdd4a069d74f50946
上海正弘建设工程顾问有限公司关于连通水系两岸（肖家村至西环路）环境整治工程中标候选人公示,江苏科晟园林景观建设集团有限公司,,1,408dd7b1135077a76169e56d0c112b0a
'''
GB_header = ['ID:ID(FZE-ID)', 'NAME', ':LABEL']
WEB_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(FZE-ID)', ':TYPE']

'''
ADDR
南京市秦淮区白鹭洲公园白鹭村1号,南京源承八九号文化艺术品投资合伙企业,1f063cd939edf1758b0542d850853a8f
洪泽县东风路84号,洪泽县四明眼镜有限公司,cb43f17129a7466593e1f1e76eea5a50
'''
DD_header = ['ID:ID(ADDR-ID)', 'NAME', ':LABEL']
RED_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(ADDR-ID)', ':TYPE']

'''
TEL,ENTNAME_GLLZD,LCID,EMAIL
025-83409842,南京源承八九号文化艺术品投资合伙企业,1f063cd939edf1758b0542d850853a8f,zxy@zixingyun.com
18021926600,洪泽县四明眼镜有限公司,cb43f17129a7466593e1f1e76eea5a50,511611378@qq.com
'''
TT_header = ['ID:ID(TEL-ID)', 'NAME', ':LABEL']
LEE1_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(TEL-ID)', 'DOMAIN', ':TYPE']

EE_header = ['ID:ID(EMAIL-ID)', 'NAME', ':LABEL']
LEE2_header = ['ID:START_ID(ENT-ID)', 'ID:END_ID(EMAIL-ID)', 'DOMAIN', ':TYPE']

files = [
    ('/home/neo4j_test/import/基本信息企业节点.csv', r'/home/neo4j_test/import/gs.csv', GS_header, 'GS', '企业节点'),
    ('/home/neo4j_test/import/人员节点-投资.csv', r'/home/neo4j_test/import/gri.csv', GR_header, 'GR', '人员节点'),
    ('/home/neo4j_test/import/人员节点-高管.csv', r'/home/neo4j_test/import/grs.csv', GR_header, 'GR', '人员节点'),
    ('/home/neo4j_test/import/企业投资.csv', r'/home/neo4j_test/import/ipees.csv', IPEES_header, 'IPEES', '投资'),
    ('/home/neo4j_test/import/自然人投资.csv', r'/home/neo4j_test/import/ipeer.csv', IPEER_header, 'IPEER', '投资'),
    ('/home/neo4j_test/import/企业分支.csv', r'/home/neo4j_test/import/bee.csv', BEE_header, 'BEE', '人员任职'),
    ('/home/neo4j_test/import/主要管理人员.csv', r'/home/neo4j_test/import/spe.csv', SPE_header, 'SPE', '专利节点'),
    ('/home/neo4j_test/import/专利节点.csv', r'/home/neo4j_test/import/pp.csv', PP_header, 'PP', '专利关系'),
    ('/home/neo4j_test/import/专利关系.csv', r'/home/neo4j_test/import/opep.csv', OPEP_header, 'OPEP', '专利关系'),
    ('/home/neo4j_test/import/诉讼节点.csv', r'/home/neo4j_test/import/ll.csv', LL_header, 'LL', '诉讼节点'),
    ('/home/neo4j_test/import/诉讼关系.csv', r'/home/neo4j_test/import/lel.csv', LEL_header, 'LEL', '诉讼关系'),
    ('/home/neo4j_test/import/招投标节点.csv', r'/home/neo4j_test/import/gb.csv', GB_header, 'GB', '招投标节点'),
    ('/home/neo4j_test/import/招投标关系.csv', r'/home/neo4j_test/import/web.csv', WEB_header, 'WEB', '招投标关系'),
    ('/home/neo4j_test/import/办公地节点.csv', r'/home/neo4j_test/import/dd.csv', DD_header, 'DD', '办公地节点'),
    ('/home/neo4j_test/import/相同办公地.csv', r'/home/neo4j_test/import/red.csv', RED_header, 'RED', '相同办公地'),
    ('/home/neo4j_test/import/电话节点.csv', r'/home/neo4j_test/import/tt.csv', TT_header, 'TT', '电话节点'),
    ('/home/neo4j_test/import/相同联系方式-电话.csv', r'/home/neo4j_test/import/lee1.csv', LEE1_header, 'LEE1', '相同联系方式'),
    ('/home/neo4j_test/import/邮箱节点.csv', r'/home/neo4j_test/import/ee.csv', EE_header, 'EE', '邮箱节点'),
    ('/home/neo4j_test/import/相同联系方式-邮箱.csv', r'/home/neo4j_test/import/lee2.csv', LEE2_header, 'LEE2', '相同联系方式'),
]


class WriteCSV(object):
    '''转化成neo4j需要的格式文件'''

    def get_id(self, name):
        return hashlib.md5(name.encode('utf8')).hexdigest()

    def GS(self, row):
        row.append('GS')
        return row

    def GR(self, row):
        row.append('GR')
        return row

    def IPEES(self, row):
        return [row[0], row[1] if row[1] else 0, row[2], row[-1], 'IPEE']

    def IPEER(self, row):
        return [row[0], row[1] if row[1] else 0, row[2], row[-1], 'IPEE']

    def BEE(self, row):
        row.append('BEE')
        return row

    def SPE(self, row):
        row.append('SPE')
        return row

    def PP(self, row):
        return [row[1], row[0], row[1], 'PP']

    def OPEP(self, row):
        return [row[1], row[0], 'OPEP']

    def LL(self, row):
        return [self.get_id(row[0]), row[0], 'LL']

    def LEL(self, row):
        return [self.get_id(row[0]), row[1], 'LEL']

    def GB(self, row):
        return [self.get_id(row[0]), row[0], 'GB']

    def WEB(self, row):
        return [row[1], self.get_id(row[0]), 'WEB']

    def DD(self, row):
        return [self.get_id(row[0]), row[0], 'DD']

    def RED(self, row):
        return [row[1], self.get_id(row[0]), 'RED']

    def TT(self, row):
        return [row[0], row[0], 'TT']

    def LEE1(self, row):
        return [row[1], row[0], row[2],'LEE']

    def EE(self, row):
        return [row[0], row[0], 'EE']

    def LEE2(self, row):
        return [row[1], row[0], row[2], 'LEE']


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
                    # print(row)
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
    # 将csv文件导入neoj
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
                 "--relationships=import/lee1.csv " \
                 "--relationships=import/lee2.csv " \
                 "--ignore-missing-nodes --ignore-duplicate-nodes --high-io=true'"
    os.system(import_cmd)
    os.system('docker restart neo4j_test')


    # 返回表中数据统计
    for key, value in report.items():
        print(key, value)


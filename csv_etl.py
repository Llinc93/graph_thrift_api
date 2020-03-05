import csv
import hashlib


class Filter(object):

    files = [
        ('/opt/csv/企业节点.csv', r'/home/neo4j_test/import/企业节点.csv', 'GS'),
        ('/opt/csv/人员节点-投资.csv', r'/home/neo4j_test/import/人员节点-投资.csv', 'GR'),
        ('/opt/csv/人员节点-高管.csv', r'/home/neo4j_test/import/人员节点-高管.csv', 'GR'),
        ('/opt/csv/企业投资.csv', r'/home/neo4j_test/import/企业投资.csv', 'IPEES'),
        ('/opt/csv/自然人投资.csv', r'/home/neo4j_test/import/自然人投资.csv', 'IPEER'),
        ('/opt/csv/企业分支.csv', r'/home/neo4j_test/import/企业分支.csv', 'BEE'),
        ('/opt/csv/主要管理人员.csv', r'/home/neo4j_test/import/主要管理人员.csv', 'SPE'),
        ('/opt/csv/专利节点', r'/home/neo4j_test/import/专利节点.csv', 'PP'),
        ('/opt/csv/专利关系.csv', r'/home/neo4j_test/import/专利关系.csv', 'OPEP'),
        ('/opt/csv/诉讼节点.csv', r'/home/neo4j_test/import/诉讼节点.csv', 'LL'),
        ('/opt/csv/诉讼关系.csv', r'/home/neo4j_test/import/诉讼关系.csv', 'LEL'),
        ('/opt/csv/招投标节点.csv', r'/home/neo4j_test/import/招投标节点.csv', 'GB'),
        ('/opt/csv/招投标关系.csv', r'/home/neo4j_test/import/招投标关系.csv', 'WEB'),
        ('/opt/csv/办公地节点.csv', r'/home/neo4j_test/import/办公地节点.csv', 'DD'),
        ('/opt/csv/相同办公地.csv', r'/home/neo4j_test/import/相同办公地.csv', 'RED'),
        ('/opt/csv/电话节点.csv', r'/home/neo4j_test/import/电话节点.csv', 'TT'),
        ('/opt/csv/相同联系方式-电话.csv', r'/home/neo4j_test/import/相同联系方式-电话.csv', 'LEE1'),
        ('/opt/csv/邮箱节点.csv', r'/home/neo4j_test/import/邮箱节点.csv', 'EE'),
        ('/opt/csv/相同联系方式-邮箱.csv', r'/home/neo4j_test/import/相同联系方式-邮箱.csv', 'LEE2'),
    ]

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

    def IPEES(self, row):
        '''
        LCID_INV、LCID
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

    def IPEER(self, row):
        '''
        PID_INV、LCID
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

    def OPEP(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
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

    def LEL(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
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
        action = [row[0], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def WEB(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
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
        action = [row[0], row[-1]]
        if '' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def RED(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
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
        action = [row[0], row[2], row[3]]
        if '' in action or '0' in action or '-' in action or '无' in action or '--' in action or '无无' in action or '0000' in action:
            flag = False
        else:
            text = ','.join(action)
        return flag, text

    def LEE1(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def EE(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
        return flag, text

    def LEE2(self, row):
        flag = True
        text = ''
        if '' in row or 'null' in row:
            flag = False
        else:
            text = ','.join(row)
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
    # Filter().run()

    import graphistry
    NEO4J_CREDS = {'uri': 'bolt://my.site.ngo:7687', 'auth': ('neo4j', 'mypwd')}
    graphistry.register(bolt=NEO4J_CREDS)
    graphistry.cypher("MATCH (a)-[p:PAYMENT]->(b) WHERE p.USD > 7000 AND p.USD < 10000 RETURN a, p, b").plot()

    graphistry.cypher("CALL db.schema()").plot()

    from neo4j import GraphDatabase, Driver
    graphistry.register(bolt=GraphDatabase.driver(**NEO4J_CREDS))
    graphistry.cypher("MATCH (a)-[p:PAYMENT]->(b) WHERE p.USD > 7000 AND p.USD < 10000 RETURN a, p, b").plot()
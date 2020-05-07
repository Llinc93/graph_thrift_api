'''
1. 循环各种规则表，构建规则字典
2. 循环企业基本信息表，构建标签字典： {lcid: [gene1, gene2]}
3. 循环各种联表，添加标签
4. 循环各种信息表，构建字段字典: {lcid: {field1: value1, field2: value2}}
5. 循环股东联表、专利联表、商标联表、企业基本信息表构造数据、导入ES
'''

import csv
import json
import time
import logging
import traceback
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class DataETL(object):

    # 索引设置
    mappings = {
        "settings": {
            "index": {
                "highlight": {
                    "max_analyzed_offset": "100000000"
                },
                "max_result_window": "1000000000",
                "refresh_interval": "-1",
                "number_of_shards": "0",
                "translog": {
                    "flush_threshold_size": "1024mb",
                    "sync_interval": "60s",
                },
                "merge": {
                    "scheduler": {
                        "max_thread_count": "2"
                    },
                    "policy": {
                        "floor_segment": "100mb",
                        "max_merged_segment": "5g"
                    }
                },
                "number_of_replicas": "0",
            }
        },
        "mappings": {
            "properties": {
                "ACTIVITY": {
                    "type": "long"
                },
                "ACTIVITY_DJ": {
                    "type": "text"
                },
                "APPRDATE": {
                    "type": "date",
                    "index": False
                },
                "APP_DESC": {
                    "type": "text",
                    "norms": False
                },
                "APP_NAME": {
                    "type": "text",
                    "norms": False
                },
                "BUSINESS": {
                    "type": "long"
                },
                "BUSINESS_DJ": {
                    "type": "text"
                },
                "CASE_TITLE": {
                    "type": "text",
                    "norms": False
                },
                "DOM": {
                    "type": "text",
                    "norms": False
                },
                "EFFICIENCY": {
                    "type": "long"
                },
                "EFFICIENCY_DJ": {
                    "type": "text"
                },
                "EMPLOYDR": {
                    "type": "long"
                },
                "EMPLOYDR_DJ": {
                    "type": "text"
                },
                "ENTINFO_NAME": {
                    "type": "text",
                    "norms": False
                },
                "ENTINFO_OLDNAME": {
                    "type": "text",
                    "norms": False
                },
                "ENTINFO_OPSCOPE": {
                    "type": "text",
                    "norms": False
                },
                "ENTINFO_GLLZD": {
                    "type": "text",
                    "norms": False
                },
                "ENTSTATUS": {
                    "type": "text",
                    "index": False
                },
                "ENTTYPE": {
                    "type": "text",
                    "index": False
                },
                "ESDATE": {
                    "type": "date"
                },
                "FRDB": {
                    "type": "text",
                    "index": False
                },
                "GENE": {
                    "type": "keyword",
                    "norms": False
                },
                "GOOD_NAME": {
                    "type": "text",
                    "norms": False
                },
                "GROWTH": {
                    "type": "long"
                },
                "GROWTH_DJ": {
                    "type": "text"
                },
                "INDUSTRYB": {
                    "type": "text",
                    "index": False
                },
                "INDUSTRYB_CODE": {
                    "type": "text",
                    "index": False
                },
                "INNOVATE": {
                    "type": "long"
                },
                "INNOVATE_DJ": {
                    "type": "text",
                    "norms": False
                },
                "INTRO": {
                    "type": "text",
                    "norms": False
                },
                "INV": {
                    "type": "text",
                    "norms": False
                },
                "JWD": {
                    "type": "geo_point"
                },
                "LCID": {
                    "type": "keyword",
                    "index": False
                },
                "OPFROM": {
                    "type": "date",
                    "index": False
                },
                "OPTO": {
                    "type": "date",
                    "index": False
                },
                "PATEN_DESC": {
                    "type": "text",
                    "norms": False
                },
                "PATEN_NAME": {
                    "type": "text",
                    "norms": False
                },
                "POSITION_DESC": {
                    "type": "text",
                    "norms": False
                },
                "POSITION_NAME": {
                    "type": "text",
                    "norms": False
                },
                "POTENTIAL": {
                    "type": "long"
                },
                "POSITION_DJ": {
                    "type": "text"
                },
                "REGCAP": {
                    "type": "long"
                },
                "REGCAPCUR": {
                    "type": "date",
                    "index": False
                },
                "REGORG": {
                    "type": "date",
                    "index": False
                },
                "RISK": {
                    "type": "long"
                },
                "RISK_DJ": {
                    "type": "text",
                    "index": False
                },
                "SHXYDM": {
                    "type": "keyword",
                    "index": False
                },
                "SOCCONTRI": {
                    "type": "long"
                },
                "SOCCONTRI_DJ": {
                    "type": "text"
                },
                "SOFTWARE_NAME": {
                    "type": "text",
                    "norms": False,
                },
                "SOFTWARE_SHORTNAME": {
                    "type": "text",
                    "norms": False,
                },
                "STRENGTH": {
                    "type": "long"
                },
                "STRENGTH_DJ": {
                    "type": "text",
                    "index": False
                },
                "TRADEMARK_NAME": {
                    "type": "text",
                    "norms": False,
                },
                "WEB_TITLE": {
                    "type": "text",
                    "norms": False,
                }
            }
        }
    }

    # 人民币汇率
    RMB_exchange_rate = {
        "阿联酋迪拉姆": 1.9092,
        "澳大利亚元": 4.6273,
        "巴西里亚尔": 1.6008,
        "加拿大元": 5.2775,
        "瑞士法郎": 7.1843,
        "丹麦克朗": 1.0209,
        "欧元": 7.6271,
        "英镑": 9.1174,
        "港币": 0.8999,
        "印尼卢比": 0.000505,
        "印度卢比": 0.097752,
        "日元": 0.0636,
        "韩国元": 0.005772,
        "澳门元": 0.8759,
        "林吉特": 1.6566,
        '挪威克朗': 0.7489,
        "新西兰元": 4.4285,
        "菲律宾比索": 0.1374,
        "卢布": 0.1074,
        "沙特里亚尔": 1.8687,
        "瑞典克朗": 0.7213,
        "新加坡元": 5.0133,
        "泰国铢": 0.2209,
        "土耳其里拉": 1.141,
        "新台币": 0.231,
        "美元": 7.0126,
        "南非兰特": 0.4608,
    }

    # 经营状态
    entstatus = {
        '在营（开业）企业': 'A1_1',
        '吊销': 'A1_1_2,A1_2',
        '注销': 'A1_1_2,A1_3',
        '吊销，已注销': 'A1_1_2,A1_3',
        '吊销，未注销': 'A1_1_2,A1_2',
    }

    # 行政区划 --- gene_district
    district = {}
    with open('', 'r', encoding='utf8') as f:
        for row in csv.reader(f):
            district['code'] = {'c1_gene': 'c1_gene', 'gene': 'gene'}

    # 经济区域 ---  d010_gene_jjqy
    jjqy = {}
    with open('', 'r', encoding='utf8') as f:
        for row in csv.reader(f):
            jjqy['name'] = 'gene'

    # 机构类型 --- d403_gene_jglx
    jglx = {}
    with open('', 'r', encoding='utf8') as f:
        for row in csv.reader(f):
            jglx['name'] = 'gene'

    # 企业类型 --- d403_gene_qylx
    qylx = {}
    with open('', 'r', encoding='utf8') as f:
        for row in csv.reader(f):
            qylx['name'] = 'gene'

    def __init__(self, ip, port, index, csv_file, file_header, log_file):
        self.es = Elasticsearch({"host": ip, "port": port})
        self.index = index

        self.file = open(csv_file, 'r', encoding='utf8')
        self.json_file = csv_file.split('.')[0] + '.json'
        self.file_header = file_header
        self.reader = csv.reader(self.file)

        self.logger = self.config_log(log_file)

        self.genes = defaultdict(list)
        self.fields = defaultdict(dict)

    def config_log(self, file):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        file_handle = logging.FileHandler(file)
        file_handle.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handle.setFormatter(formatter)

        logger.addHandler(file_handle)
        return logger

    def __del__(self):
        self.file.close()
        print(f'{self.file} close')

    def jichu_tag(self, action):
        # 基础标签
        gene = []

        # step 1.1 经营状况标签
        gene.extend(self.entstatus[action['ENTSTATUS']].split(','))

        # step 1.2 企业成立时间标签和企业死亡时间标签
        for item in [action['ESDATE'], action['ENDDATE']]:
            year = int(item[0:4])
            month = item[5:7]

            if year > 2010:
                gene.append(f'A6_{year}')
            elif year > 2000:
                gene.append('A6_2010-LAST')
                gene.append(f'A6_{year}')
            elif year > 1990:
                gene.append('A6_2000-LAST')
                gene.append(f'A6_{year}')
            elif year > 1980:
                gene.append('A6_1990-LAST')
                gene.append(f'A6_{year}')
            elif year >= 1978:
                gene.append('A6_1990-LAST')
                gene.append(f'A6_{year}')
            else:
                gene.append(f'A6_1978-LAST')

            if year >= 2011:
                gene.append(f'A6_{year}_{month}')

        # step 1.3 注册资本
        regcap = float(action['REGCAP'])
        if regcap > 1:
            if regcap > 100000:
                gene.append('A8_39')
            # elif regcap > 90000:
            #     gene.append('Tag35')
            #     gene.append('A8_38')
            # elif regcap > 80000:
            #     gene.append('Tag35')
            #     gene.append('A8_37')
            # elif regcap > 70000:
            #     gene.append('Tag35')
            #     gene.append('A8_36')
            # elif regcap > 60000:
            #     gene.append('Tag35')
            #     gene.append('A8_35')
            # elif regcap > 50000:
            #     gene.append('Tag35')
            #     gene.append('A8_34')
            elif regcap > 50000:
                gene.append('Tag35')
                gene.append(f'A8_3{regcap // 10000 - 1}')
            # elif regcap > 40000:
            #     gene.append('Tag34,A8_33')
            # elif regcap > 30000:
            #     gene.append('Tag34,A8_32')
            # elif regcap > 20000:
            #     gene.append('Tag34,A8_31')
            # elif regcap > 10000:
            #     gene.append('Tag34,A8_30')
            elif regcap > 10000:
                # gene.append('Tag34,A8_30')
                gene.append('Tag34')
                gene.append(f'A8_3{regcap // 10000 - 1}')
            # elif regcap > 9000:
            #     gene.append('Tag33,A8_29')
            # elif regcap > 8000:
            #     gene.append('Tag33,A8_28')
            # elif regcap > 7000:
            #     gene.append('Tag33,A8_27')
            # elif regcap > 6000:
            #     gene.append('Tag33,A8_26')
            # elif regcap > 5000:
            #     gene.append('Tag33,A8_25')
            elif regcap > 5000:
                # gene.append('Tag33,A8_25')
                gene.append('Tag33')
                gene.append(f'A8_2{regcap // 1000}')
            # elif regcap > 4000:
            #     gene.append('Tag32,A8_24')
            # elif regcap > 3000:
            #     gene.append('Tag32,A8_23')
            # elif regcap > 2000:
            #     gene.append('Tag32,A8_22')
            # elif regcap > 1000:
            #     gene.append('Tag32,A8_21')
            elif regcap > 1000:
                # gene.append('Tag32,A8_21')
                gene.append('Tag32')
                gene.append(f'A8_2{regcap // 1000}')
            elif regcap > 900:
                # gene.append('Tag31,A8_20')
                gene.append('Tag31')
                gene.append('A8_20')
            # elif regcap > 800:
            #     gene.append('Tag31,A8_19')
            # elif regcap > 700:
            #     gene.append('Tag31,A8_18')
            # elif regcap > 600:
            #     gene.append('Tag31,A8_17')
            # elif regcap > 500:
            #     gene.append('Tag31,A8_16')
            elif regcap > 500:
                # gene.append('Tag31,A8_16')
                gene.append('Tag31')
                gene.append(f'A8_1{regcap // 100 + 1}')
            # elif regcap > 400:
            #     gene.append('Tag30,A8_15')
            # elif regcap > 300:
            #     gene.append('Tag30,A8_14')
            # elif regcap > 200:
            #     gene.append('Tag30,A8_13')
            # elif regcap > 100:
            #     gene.append('Tag30,A8_12')
            elif regcap > 100:
                # gene.append('Tag30,A8_12')
                gene.append('Tag30')
                gene.append(f'A8_1{regcap // 100 + 1}')
            # elif regcap > 90:
            #     gene.append('Tag29,A8_11')
            # elif regcap > 80:
            #     gene.append('Tag29,A8_10')
            # elif regcap > 70:
            #     gene.append('Tag29,A8_9')
            # elif regcap > 60:
            #     gene.append('Tag29,A8_8')
            # elif regcap > 50:
            #     gene.append('Tag29,A8_7')
            # elif regcap > 40:
            #     gene.append('Tag29,A8_6')
            # elif regcap > 30:
            #     gene.append('Tag29,A8_5')
            # elif regcap > 20:
            #     gene.append('Tag29,A8_4')
            # elif regcap > 10:
            #     gene.append('Tag29,A8_3')
            else:
                # gene.append('Tag29,A8_2')
                gene.append('Tag29')
                gene.append(f'A8_{regcap // 10 + 2}')

        # step 1.4 行政区划标签和经济区域标签
        if action['DISTRICT'] in self.district:
            gene.extend(self.district[action['DISTRICT']]['c1_gene'].split(','))
            for code in self.district[action['DISTRICT']]['gene'].split(','):
                if code in self.jjqy:
                    gene.extend(self.jjqy['code'].split(','))

        # step 1.5 机构类型标签和企业类型标签
        if action['ENTTYPE'] in self.jglx:
            gene.extend(self.jglx[action['ENTTYPE']].split(','))
            gene.extend(self.qylx[action['ENTTYPE']].split(','))
        self.genes[action['LCID']].extend(gene)
        return action

    def zl_tag(self, action):
        pass
        return action

    def rz_tag(self, action):
        pass
        return action

    def sb_tag(self, action):
        pass
        return action

    def zpzzq_tag(self, action):
        pass
        return action

    def app_tag(self, action):
        pass
        return action

    def jck_tag(self, action):
        pass
        return action

    def guimo_tag(self, action):
        pass
        return action

    def ss_tag(self, action):
        pass
        return action

    def isbranch_tag(self, action):
        pass
        return action

    def havebranch_tag(self, action):
        pass
        return action

    def havezp_tag(self, action):
        pass
        return action

    def gw_tag(self, action):
        pass
        return action

    def add_tag(self, action):
        self.jichu_tag(action)
        self.zl_tag(action)
        self.rz_tag(action)
        self.sb_tag(action)
        self.zpzzq_tag(action)
        self.app_tag(action)
        self.jck_tag(action)
        self.guimo_tag(action)
        self.ss_tag(action)
        self.isbranch_tag(action)
        self.havebranch_tag(action)
        self.havezp_tag(action)
        self.gw_tag(action)
        return action

    ##################################


    def create(self):
        try:
            if self.es.indices.exists(index=self.index):
                self.logger.info(f'索引{self.index}已存在')
            else:
                ret = self.es.indices.create(index=self.index, body=self.mappings)
                self.logger.info(f'创建索引{self.index}：{ret}')
        except:
            error = traceback.format_exc()
            self.logger.error(f'创建索引失败：{error}')

    def bulk(self, doc):
        try:
            ret = bulk(self.es, actions=doc, index='gudong', stats_only=True, chunk_size=1000)
            self.logger.info(f'bulk：{ret}')
        except:
            error = traceback.format_exc()
            self.logger.error(f'bulk失败：{error}')

    def run(self):
        f = open(self.json_file, 'w', encoding='utf8')
        for row in self.reader:
            try:
                action = dict(zip(file_header, row))

                # step 1 构建标签字典
                action = self.add_tag(action)

                # step 2 构建字段字典
                self.add_field(action)

                # step 3 循环企业基本信息表，导入ES

                # step 4 循环股东与企业基本信息链表，导入ES

                # step 5 循环专利与企业基本信息链表，导入ES

                # step 6 循环商标与企业基本信息链表，导入ES

                # step 7 循环软著与企业基本信息链表，导入ES
            except:
                error = traceback.format_exc()
                self.logger.error(f'etl失败：{error} --- 原文：{json.dumps(row, ensure_ascii=False)}')
        f.close()


if __name__ == '__main__':
    start = time.time()
    ip = "172.27.2.4"
    port = 8100
    index = 'gudong'
    csv_file = 'gudong.csv'
    label = None
    file_header = [
        'acconam', 'blicno', 'blictype', 'condate', 'invname', 'invtype', 'lcid', 'lcid_inv', 'pid_inv',
        'province_inv', 'subconam', 'rate', 'apprdate', 'candate', 'district', 'dom', 'enddate', 'entname',
        'entstatus', 'enttype', 'esdate', 'industry', 'name', 'opfrom', 'opscope', 'opto', 'regcap', 'reccapcur',
        'regno', 'regorg', 'revdate', 'province', 'uniscid'
    ]
    file_header = [item.upper() for item in file_header]
    log_file = 'gudong.log'
    etl = DataETL(ip, port, index, csv_file, file_header, log_file)
    etl.run()

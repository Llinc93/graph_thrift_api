import csv
import json
import time
import logging
import traceback
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
                "number_of_shards": "0",
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

                # 股东表
                "ACCONAM": {
                    "type": float
                },
                "CONDATE": {
                    "type": "date"
                },
                "INVNAME": {
                    "type": "keyword",
                    "index": False
                },
                "INVTYPE": {
                    "type": "text",
                    "index": False
                },
                "LCID_INV": {
                    "type": "keyword",
                    "index": False,
                },
                "PID_INV": {
                    "type": "keyword",
                    "index": False,
                }
                "SUBCONAM": {
                    "type": float,
                    "index": False,
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
        self.mappings = {}
        self.file = open(csv_file, 'r', encoding='utf8')
        self.json_file = csv_file.split('.')[0] + '.json'
        self.file_header = file_header
        self.reader = csv.reader(self.file)
        self.logger = self.config_log(log_file)

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
        action['GENE'] = []

        # step 1.1 经营状况标签
        action['GENE'].append(self.entstatus[action['ENTSTATUS']])

        # step 1.2 企业成立时间标签和企业死亡时间标签
        for item in [action['ESDATE'], action['ENDDATE']]:
            year = int(item[0:4])
            month = item[5:7]

            if year > 2010:
                action['GENE'].append(f'A6_{year}')
            elif year > 2000:
                action['GENE'].append(f'A6_2010-LAST,A6_{year}')
            elif year > 1990:
                action['GENE'].append(f'A6_2000-LAST,A6_{year}')
            elif year > 1980:
                action['GENE'].append(f'A6_1990-LAST,A6_{year}')
            elif year >= 1978:
                action['GENE'].append(f'A6_1990-LAST,A6_{year}')
            else:
                action['GENE'].append(f'A6_1978-LAST')

            if year >= 2011:
                action['GENE'].append(f'A6_{year}_{month}')

        # step 1.3 注册资本
        regcap = float(action['REGCAP'])
        if regcap > 1:
            if regcap > 100000:
                action['GENE'].append('A8_39')
            elif regcap > 90000:
                action['GENE'].append('Tag35,A8_38')
            elif regcap > 80000:
                action['GENE'].append('Tag35,A8_37')
            elif regcap > 70000:
                action['GENE'].append('Tag35,A8_36')
            elif regcap > 60000:
                action['GENE'].append('Tag35,A8_35')
            elif regcap > 50000:
                action['GENE'].append('Tag35,A8_34')
            elif regcap > 40000:
                action['GENE'].append('Tag34,A8_33')
            elif regcap > 30000:
                action['GENE'].append('Tag34,A8_32')
            elif regcap > 20000:
                action['GENE'].append('Tag34,A8_31')
            elif regcap > 10000:
                action['GENE'].append('Tag34,A8_30')
            elif regcap > 9000:
                action['GENE'].append('Tag33,A8_29')
            elif regcap > 8000:
                action['GENE'].append('Tag33,A8_28')
            elif regcap > 7000:
                action['GENE'].append('Tag33,A8_27')
            elif regcap > 6000:
                action['GENE'].append('Tag33,A8_26')
            elif regcap > 5000:
                action['GENE'].append('Tag33,A8_25')
            elif regcap > 4000:
                action['GENE'].append('Tag32,A8_24')
            elif regcap > 3000:
                action['GENE'].append('Tag32,A8_23')
            elif regcap > 2000:
                action['GENE'].append('Tag32,A8_22')
            elif regcap > 1000:
                action['GENE'].append('Tag32,A8_21')
            elif regcap > 900:
                action['GENE'].append('Tag31,A8_20')
            elif regcap > 800:
                action['GENE'].append('Tag31,A8_19')
            elif regcap > 700:
                action['GENE'].append('Tag31,A8_18')
            elif regcap > 600:
                action['GENE'].append('Tag31,A8_17')
            elif regcap > 500:
                action['GENE'].append('Tag31,A8_16')
            elif regcap > 400:
                action['GENE'].append('Tag30,A8_15')
            elif regcap > 300:
                action['GENE'].append('Tag30,A8_14')
            elif regcap > 200:
                action['GENE'].append('Tag30,A8_13')
            elif regcap > 100:
                action['GENE'].append('Tag30,A8_12')
            elif regcap > 90:
                action['GENE'].append('Tag29,A8_11')
            elif regcap > 80:
                action['GENE'].append('Tag29,A8_10')
            elif regcap > 70:
                action['GENE'].append('Tag29,A8_9')
            elif regcap > 60:
                action['GENE'].append('Tag29,A8_8')
            elif regcap > 50:
                action['GENE'].append('Tag29,A8_7')
            elif regcap > 40:
                action['GENE'].append('Tag29,A8_6')
            elif regcap > 30:
                action['GENE'].append('Tag29,A8_5')
            elif regcap > 20:
                action['GENE'].append('Tag29,A8_4')
            elif regcap > 10:
                action['GENE'].append('Tag29,A8_3')
            else:
                action['GENE'].append('Tag29,A8_2')

        # step 1.4 行政区划标签和经济区域标签
        if action['DISTRICT'] in self.district:
            action['GENE'].append(self.district[action['DISTRICT']]['c1_gene'])
            for code in self.district[action['DISTRICT']]['gene'].split(','):
                if code in self.jjqy:
                    action['GENE'].append(self.jjqy['code'])

        # step 1.5 机构类型标签和企业类型标签
        if action['ENTTYPE'] in self.jglx:
            action['GENE'].append(self.jglx[action['ENTTYPE']])
            action['GENE'].append(self.qylx[action['ENTTYPE']])

        return action

    def add_tag(self, action):
        action = self.jichu_tag(action)
        return action

    def add_field(self, action):
        '''
        ES中企业基本信息 字段
            SOFTWARE_SHORTNAME   ENTSTATUS   INDUSTRY_CODE   APP_NAME   BUSINESS   REGORG   ACTIVITY_DJ
            INNOVATE_DJ   JWD   GROWTH   SOFTWARE_NAME   GROWTH_DJ   GOOD_NAME   ENTINFO_OLDNAME   ENTINFO_OPSCOPE
            DOM   REGCAPCUR   INDUSTRYB   STRENGTH   STRENGTH_DJ   ESDATE   GENE   OPTO   PATEN_DESC   BUSINESS_DJ
            WEB_TITLE   PATEN_NAME   TRADEMARK_NAME   EMPLOYDR_DJ   INV   SOCCONTRI   EMPLOYDR   APPRDATE   OPFROM
            SHXYDM   EFFICIENCY   POSITION_DESC   EFFICIENCY_DJ   ENTNAME_GLLZD   REGCAP   POTENTIAL   INTRO   RISK
            ENTTYPE   FRDB   POSITION_NAME   CASE_TITLE   LCID   APP_DESC   RISK_DJ   POTENTIAL_DJ   INNOVATE
            SOCCONTRI_DJ   ENTINFO_NAME   ACTIVITY
        ES --- 企业基本信息表字段部分
            APPRDATE --- apprdate
            DOM --- DOM
            ENTINFO_NAME --- entname
            ENTNAME_GLLZD --- entname
            ENTSTATUS --- entstatus
            ENTTYPE --- enttype
            ESDATE --- esdate
            INDUSTRY_CODE --- industry
            ENTINFO_OPSCOPE --- opscope
            OPFROM --- opfrom
            OPTO --- opto
            REGCAP --- regcap
            REGCAPCUR --- reccapcur
            REGORG --- regorg
            LCID --- lcid

        ES --- 股东表字段部分


        '''
        file_header = [
            'acconam', 'blicno', 'blictype', 'condate', 'invname', 'invtype', 'lcid', 'lcid_inv', 'pid_inv',
            'province_inv', 'subconam', 'rate', 'apprdate', 'candate', 'district', 'dom', 'enddate', 'entname',
            'entstatus', 'enttype', 'esdate', 'industry', 'name', 'opfrom', 'opscope', 'opto', 'regcap', 'reccapcur',
            'regno', 'regorg', 'revdate', 'province', 'uniscid'
        ]
        data = {
            'ACCONAM': action['acconam'],
            'CONDATE': action['condate'],
            'INVNAME': action['invname'],
            'INVTYPE': action['invtype'],
            'LCID_INV': action['lcid_inv'],
            'PID_INV': action['pid_inv'],
            'SUBCONAM': float(action['subconam']) * self.RMB_exchange_rate.get(action['reccapcur'], 1.0)
            'RATE': action['rate'],

            "APPRDATE": action["apprdate"],
            'DOM': action['dom'],
            'ENTINFO_NAME': action['entname'],
            'ENTNAME_GLLZD': action['entname'],
            'ENTSTATUS': action['entstatus'],
            'ENTTYPE': action['enttype'],
            'ESDATE': action['esdate'],
            'INDUSTRY_CODE': action['industry'],
            'ENTINFO_OPSCOPE': action['opscope'],
            'OPFROM': action['opfrom'],
            'OPTO': action['opto'],
            'REGCAP': action['regcap'],
            'REGCAPCUR': action['reccapcur'],
            'REGORG': action['regorg'],
            'LCID': action['lcid'],
            'SOFTWARE_SHORTNAME': None,
            'APP_NAME': None,
            'BUSINESS': None,
            'ACTIVITY_DJ': None,
            'INNOVATE_DJ': None,
            'JWD': None,
            'GROWTH': None,
            'SOFTWARE_NAME': None,
            'GROWTH_DJ': None,
            'GOOD_NAME': None,
            'ENTINFO_OLDNAME': None,
            'INDUSTRYB': None,
            'STRENGTH': None,
            'STRENGTH_DJ': None,
            'GENE': action['GENE'],
            'PATEN_DESC': None,
            'BUSINESS_DJ': None,
            'WEB_TITLE': None,
            'PATEN_NAME': None,
            'TRADEMARK_NAME': None,
            'EMPLOYDR_DJ': None,
            'INV': None,
            'SOCCONTRI': None,
            'EMPLOYDR': None,
            'SHXYDM': None,
            'EFFICIENCY': None,
            'POSITION_DESC': None,
            'EFFICIENCY_DJ': None,
            'POTENTIAL': None,
            'INTRO': None,
            'RISK': None,
            'FRDB': None,
            'POSITION_NAME': None,
            'CASE_TITLE': None,
            'APP_DESC': None,
            'RISK_DJ': None,
            'POTENTIAL_DJ': None,
            'INNOVATE': None,
            'SOCCONTRI_DJ': None,
            'ACTIVITY': None,
        }
        return data

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
        f = open(self.json_file + "test", 'w', encoding='utf8')
        index = 1
        for row in self.reader:
            try:
                action = dict(zip(file_header, row))
                action = self.add_tag(action)
                action = self.add_field(action)
                f.write(json.dumps({"index": {}}, ensure_ascii=False))
                f.write('\n')
                f.write(json.dumps(action, ensure_ascii=False))
                f.write('\n')
                if index == 20000:
                    break
                index += 1
            except:
                error = traceback.format_exc()
                self.logger.error(f'etl失败：{error} --- 原文：{json.dumps(row, ensure_ascii=False)}')
        f.close()

    def import_es(self):
        start = time.time()
        self.create()
        doc = []
        with open(self.json_file, 'r', encoding='utf8') as f:
            index = 0
            for line in f:
                index += 1
                doc.append(json.loads(line.strip()))
                if index == 20000:
                    time.time()
                    self.bulk(doc)
                    break
        print(f'{self.index}耗时：{time.time() - start}s')

    def run2(self):
        start = time.time()
        self.create()
        f = open(self.json_file, 'w', encoding='utf8')
        doc = []
        index = 0
        for row in self.reader:
            index += 1
            try:
                action = dict(zip(file_header, row))
                action = self.add_tag(action)
                doc.append({"index": {}})
                doc.append(action)
                f.write(json.dumps({"index": {}}, ensure_ascii=False))
                f.write('\n')
                f.write(json.dumps(action, ensure_ascii=False))
                f.write('\n')
                if index == 100000:
                    self.bulk(doc)
                    doc = []
            except:
                error = traceback.format_exc()
                self.logger.error(f'etl失败：{error} --- 原文：{json.dumps(row, ensure_ascii=False)}')
        f.close()
        if doc:
            self.bulk(doc)
        print(f'{self.index}耗时：{time.time() - start}s')


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
    log_file = 'gudong.log'
    etl = DataETL(ip, port, index, csv_file, file_header, log_file)
    etl.run()

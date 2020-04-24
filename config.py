# neo4j
NEO4J_URL = 'http://localhost:17474'
# NEO4J_URL = 'http://47.93.228.56:9920'
NEO4J_USER = 'neo4j'
NEO4J_PASSWD = '123456'


# relation map
RELATIONSHIP_MAP = {
    'IPEE': '投资',
    'SPE': '任职',
    'BEE': '开设分支',
    'WEB': '招投标',
    'RED': '相同办公地',
    'LEE': '共同联系方式',
    'OPEP': '共有专利',
    'LEL': '诉讼',
    'IHPEEN': '历史投资',
    'SHPEN': '历史任职',
}


# ATTIDS MAP
ATTIDS_MAP = {
    'R101': {'ipees': 'IPEES'},  # 企业对外投资
    'R102': {'rev_ipees': 'REV_IPEES'},  # 企业股东
    'R103': {'ipeer': 'IPEER'},  # 自然人对外投资
    'R104': {'rev_ipeer': 'REV_IPEER'},  # 自然人股东
    'R105': {'spe': 'SPE'},  # 管理人员其他公司任职
    'R106': {'rev_spe': 'REV_SPE'},  # 公司管理人员
    'R107': {'bee': 'BEE'},  # 分支机构
    'R108': {'rev_bee': 'REV_BEE'},  # 总部
    'R109': {'web': 'WEB'},  # 企业关联中标
    'R110': {'rev_web': 'REV_WEB'},  # 中标关联企业
    'R111': {'red': 'RED'},  # 企业关联注册地
    'R112': {'rev_red': 'REV_RED'},  # 注册地关联企业
    'R113': {'leee': 'LEEE', 'leet': 'LEET'},  # 企业关联邮箱 / 电话
    'R114': {'rev_leee': 'REV_LEEE', 'rev_leet': 'REV_LEET'},  # 邮箱 / 电话关联企业
    'R115': {'opep': 'OPEP'},  # 企业关联专利
    'R116': {'rev_opep': 'REV_OPEP'},  # 专利关联企业
    'R117': {'lel': 'LEL'},  # 企业关联诉讼
    'R118': {'rev_lel': 'REV_LEL'},  # 诉讼关联企业
    # 'R119': {'n': '', 'r': '', 'd': 3},     # 人员关联专利
    # 'R120': {'n': '', 'r': '', 'd': 3},     # 专利关联人员
    # 'R139': {'n': '', 'r': '', 'd': 3},     # 历史企业股东
    # 'R140': {'n': '', 'r': '', 'd': 3},     # 历史企业对外投资
    # 'R141': {'n': '', 'r': '', 'd': 3},     # 历史自然人股东
    # 'R142': {'n': '', 'r': '', 'd': 3},     # 历史自然人对外投资
    # 'R143': {'n': '', 'r': '', 'd': 3},     # 历史公司管理人员
    # 'R144': {'n': '', 'r': '', 'd': 3},     # 历史管理人员其他公司任职
}

LINK_NAME = {
    'IPEES': '投资',
    'IPEER': '投资',
    'SPE': '任职',
    'BEE': '分支机构',
    'WEB': '招投标',
    'RED': '相同办公地',
    'LEEE': '共同联系方式',
    'LEET': '共同联系方式',
    'OPEP': '共有专利',
    'LEL': '诉讼',
    'IHPEEN': '历史投资',
    'SHPEN': '历史任职',
}

# TigerGraph
EntActualController = 'http://172.27.2.5:9000/query/graph_api/EntActualContoller'
# EntFinalBeneficiaryName = 'http://172.27.2.2:9000/query/graph_api/EntFinalBeneficiaryName'
EntFinalBeneficiaryName = 'http://172.27.2.2:9000/query/graph_api/EntFinalBeneficiaryNameV2'
EntGraphUrl = 'http://172.27.2.5:9000/query/graph_api/EntGraph'
EntRelevanceSeekGraphUrl = 'http://172.27.2.5:9000/query/graph_api/EntsRelevanceSeekGraph'
EntRelevanceSeekGraphUrl_v2 = 'http://172.27.2.5:9000/query/graph_api/EntsRelevanceSeekGraphV2'
EntsDegreeCompare = 'http://172.27.2.5:9000/query/graph_api/EntsDegreeCompare'

# FLASK、GUNICORN
HOST = '0.0.0.0'
PORT = 8140
DEBUG = True


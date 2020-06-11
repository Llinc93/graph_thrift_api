# neo4j
NEO4J_URL = 'http://localhost:7474'
NEO4J_USER = 'neo4j'
NEO4J_PASSWD = '123456'


RELATION_MAP = {
    'IPEE': '投资',
    'IPEES': '投资',
    'IPEER': '投资',
    'SPE': '任职',
    'BEE': '分支机构',
    'WEB': '招投标',
    'RED': '相同办公地',
    'LEE': '共同联系方式',
    'OPEP': '共有专利',
    'LEL': '诉讼',
    'IHPEEN': '历史投资',
    'SHPEN': '历史任职',
}


# FLASK、GUNICORN
HOST = '0.0.0.0'
PORT = 8140
DEBUG = True


# 缓存设置
CACHE_FLAG = True

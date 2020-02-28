# neo4j
# NEO4J_URL = 'http://localhost:7474'
NEO4J_URL = 'http://47.93.228.56:9920'
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

if __name__ == '__main__':
    s = '%E6%B1%9F%E8%8B%8F%E8%8D%A3%E9%A9%AC%E5%AE%9E%E4%B8%9A%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8;%E6%B1%9F%E8%8B%8F%E8%8D%A3%E9%A9%AC%E5%9F%8E%E5%B8%82%E5%BB%BA%E8%AE%BE%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8'
    import urllib
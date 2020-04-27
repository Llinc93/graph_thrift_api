import time
import redis


pool = redis.ConnectionPool(host='localhost', port=6379, db=7, decode_responses=True)
r = redis.Redis(connection_pool=pool)
r.flushdb()

file = '/home/tigergraph/data/gs.csv'

count = 0
s = time.time()
with open(file, 'r', encoding='utf8') as f:
    for line in f:
        row = line.strip().split(',')
        r.set(row[1].strip(), row[0].strip())
        count += 1
print(f'共导入{count}个企业节点，耗时{time.time() - s}秒！')
print('END!')

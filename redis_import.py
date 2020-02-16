import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
r = redis.Redis(connection_pool=pool)
r.flushdb()

node_level_path = '/opt/graph_thrift_api/data/ent_level_{}.csv'

for index in range(3, 10):
    with open(node_level_path.format(index), 'r', encoding='utf8') as f:
        for line in f:
            lcid, level, tmp = line.strip().split(',')
            r.set(lcid, level)
print('END')

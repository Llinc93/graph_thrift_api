import time
import redis
from multiprocessing import Pool


def task(items, r):
    for lcid, name in items:
        r.set(name, lcid)
    return None


if __name__ == '__main__':
    p = Pool(10)
    pool = redis.ConnectionPool(host='localhost', port=6379, db=7, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    r.flushdb()

    file = '/home/tigergraph/data/gs.csv'

    count = 0
    s = time.time()
    items = []
    with open(file, 'r', encoding='utf8') as f:
        for line in f:
            count += 1
            row = line.strip().split(',')
            items.append((row[1], row[0]))
            if count % 100000 == 0:
                p.apply_async(func=task, args=(items, r))
                items = []
        else:
            p.apply_async(func=task, args=(items, r))

    p.close()
    p.join()
    print(f'共导入{count}个企业节点，耗时{time.time() - s}秒！')
    print('END!')

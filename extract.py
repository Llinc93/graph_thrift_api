import os
import time
import py2neo
from multiprocessing import Pool


def extract_from_neo4j(csv_path, index, neo4j_uri, username, password):
    '''
    从neo4j中提取数据
    :param csv_path:
    :param index:
    :param neo4j_uri:
    :param username:
    :param password:
    :return:
    '''
    print(f'进程{os.getpid()}：正在查询第{index}层企业节点')
    s = time.time()
    wf = open(csv_path, 'w', encoding='utf8')
    if index != 10:
        command = 'match (n:GS) -[:IPEER|:IPEES|:BEE* %s .. %s]-> (m:GS) where not (:GS) -[:IPEES|:IPEER|:BEE]-> (n) return distinct m.ID as nid'
    else:
        command = 'match (n:GS) -[:IPEER|:IPEES|:BEE* %s .. %s]-> (m:GS) return distinct m.ID as nid'

    graph = py2neo.Graph(uri=neo4j_uri, username=username, password=password)
    ret = graph.run(command % (index, index)).data()

    count = 0
    for i in ret:
        count += 1
        wf.write(','.join([i['nid'], str(index), '\n']))
    wf.close()
    print(f'{index}层企业查询完毕: {count},耗时{time.time()-s}秒！')
    return None


if __name__ == '__main__':

    p = Pool(3)

    path = '/opt/graph_thrift_api/data'
    tmp_name = 'ent_level_{}.csv'
    neo4j_uri = 'http://localhost:7474'
    username = 'neo4j'
    password = '123456'

    s = time.time()
    if not os.path.exists(path):
        os.makedirs(path)

    for index in range(3, 11):
        csv_path = os.path.join(path, tmp_name.format(index))
        p.apply_async(func=extract_from_neo4j, args=(csv_path, index, neo4j_uri, username, password))

    p.close()
    p.join()
    print(f'提取企业节点完成，一共耗时{time.time() - s}秒！')
    print('End!')
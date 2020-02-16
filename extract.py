import os
import py2neo


path = '/opt/graph_thrift_api/data'
tmp_name = 'ent_level_{}.csv'
neo4j_uri = 'http://172.27.2.2:7474'
username = 'neo4j'
password = '123456'


if not os.path.exists(path):
    os.makedirs(path)


for index in range(3, 10):
    wf = open(os.path.join(path, tmp_name.format(index)), 'a', encoding='utf8')
    if index != 9:
        command = 'match (n:GS) -[:IPEE* %s .. %s]-> (m:GS) where not (:GS) -[:IPEE]-> (n) return distinct m.ID as nid'
    else:
        command = 'match (n:GS) -[:IPEE* %s .. %s]-> (m:GS) return distinct m.ID as nid'

    graph = py2neo.Graph(uri=neo4j_uri, username=username, password=password)
    ret = graph.run(command % (index, index)).data()

    count = 0
    for i in ret:
        count += 1
        wf.write(','.join([i['nid'], str(index), '\n']))
    wf.close()
    print(f'{index}层企业查询完毕: {count}')
print('END')
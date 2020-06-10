import sys
sys.setrecursionlimit(100000)
import json
from py2neo import Graph
from collections import defaultdict
from copy import deepcopy


def test(nodes, link_index, links, p_node, target, exists_ids):
    '''

    :param nodes:  节点列表
    :param link_index: 关系嵌套顺序字典
    :param links:  关系列表
    :param p_node: 起点
    :param target: 目标节点
    :param exists_node:  已出现节点列表
    :return:
    '''
    data = []
    exists_ids.append(p_node['ID'])
    number_total = 0

    for s_node_id in link_index[p_node['ID']]:
        snode = nodes[s_node_id]
        s_node = {
            "number": 0,
            "number_c": float(links[(p_node['ID'], snode['ID'])]['RATE']),
            "children": None,
            "lastnode": 0,
            "name": snode['NAME'],
            "pid": p_node['ID'],
            "id": snode['ID'],
            "type": snode['label'],
            "attr": 1,
        }
        if s_node_id == target['ID']:
            s_node['attr'] = 2
            data.append(s_node)
            number_total += 1 * s_node['number_c']
            continue

        if s_node_id in exists_ids:
            continue

        children, number = test(nodes, link_index, links, snode, target, deepcopy(exists_ids))
        s_node['children'] = children
        s_node['number'] = number
        number_total += s_node['number_c'] * number
        data.append(s_node)
    return data, number_total


graph = Graph(uri='http://118.192.47.57:7474')

# command = "match p = (n) -[r:TIPEES* 1 .. 10]-> (m:TEST {name: 'a'}) return distinct [node in nodes(p) | properties(node)] as n, [link in relationships(p) | properties(link)] as r"
command = "match p = (n) -[r:IPEES|:IPEER|:BEE* 1 .. 10]-> (m:GS {ID: '563f1b06607558deed6109f21ed651ef'}) return distinct [node in nodes(p) | properties(node)] as n, [link in relationships(p) | properties(link)] as r"
rs = graph.run(command)
path_set = rs.data()
rs.close()


inside = defaultdict(int)
outside = {}
nodes = {}
links = {}
link_index = defaultdict(set)
print(path_set)
for index, path in enumerate(path_set, 1):
    repeat = set()
    flag = False
    for item in path['n']:
        if item['ID'] in repeat:
            flag = True
            break
        else:
            repeat.add(item['ID'])
    if flag:
        continue

    outside[path['n'][0]['ID']] = path['n'][0]
    nodes[path['n'][0]['ID']] = path['n'][0]
    for item in path['n'][1:]:
        inside[item['ID']] += 1
        nodes[item['ID']] = item

    for link in path['r']:
        links[(link['pid'], link['id'])] = link
        link_index[link['pid']].add(link['id'])

    # print(index, [i['name'] for i in path['n']], len(path['n']) - 1)
else:
    target = path['n'][-1]


# link_desc = defaultdict(set)
# for key, value in link_index.items():
#     link_desc[nodes[key]['NAME']] = [nodes[i]['NAME'] for i in value]

# print(target)
# print(outside)
print(nodes)
# print(link_index)
# print(link_desc)
# print(links, len(links))

data = []
for key, value in outside.items():
    if key in inside:
        continue
    exists = []
    # print(value)
    children, number = test(nodes, link_index, links, value, target, exists)
    if number < 0 and value['label'] == 'GR':
        continue
    action = {
        "number": number,
        "number_c": None,
        "children": children,
        "lastnode": 1,
        "name": value['NAME'],
        "pid": None,
        "id": value['ID'],
        "type": value['label'],
        "attr": 1,
    }
    data.append(action)
    # break
print(data)


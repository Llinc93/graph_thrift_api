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
    exists_ids.append(p_node['name'])

    number_total = 0
    for s_node_id in link_index[p_node['id']]:
        # print(p_node['name'], exists_ids)
        s_node = nodes[s_node_id]
        # print('s_node', s_node['name'], s_node_id)

        if s_node_id == target['id']:
            s_node['children'] = None
            s_node['number'] = 0
            data.append(s_node)
            print('s_node', s_node['name'], exists_ids)
            return data, 1

        # if s_node_id in exists_ids:
        #     continue
        if s_node['name'] in exists_ids:
            continue

        s_node['pid'] = p_node['id']
        s_node['number_c'] = links[(p_node['id'], s_node_id)]['rate']

        children, number = test(nodes, link_index, links, s_node, target, deepcopy(exists_ids))
        s_node['children'] = children
        s_node['number'] = number * s_node['number_c']
        number_total += s_node['number']

        data.append(s_node)
        print()
    return data, number_total


def test2(nodes, link_index, links, p_node, target, exists_ids):
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
    exists_ids.append(p_node['name'])

    number_total = 0
    for s_node_id in link_index[p_node['id']]:
        # print(p_node['name'], exists_ids)
        s_node = nodes[s_node_id]
        # print('s_node', s_node['name'], s_node_id)
        if s_node_id == target['id']:
            s_node['children'] = None
            s_node['number'] = 0
            data.append(s_node)
            exists_ids.pop()
            print(p_node['name'], exists_ids)
            return data, 1

        # if s_node_id in exists_ids:
        #     exists_ids.pop()
        #     continue
        if s_node['name'] in exists_ids:
            # exists_ids.pop()
            continue

        s_node['pid'] = p_node['id']
        s_node['number_c'] = links[(p_node['id'], s_node_id)]['rate']

        children, number = test2(nodes, link_index, links, s_node, target, exists_ids)
        s_node['children'] = children
        s_node['number'] = number * s_node['number_c']
        number_total += s_node['number']

        data.append(s_node)
        print()
    exists_ids.pop()
    return data, number_total


graph = Graph(uri='http://118.192.47.57:7474')

command = "match p = (n) -[r:TIPEES* 1 .. 10]-> (m:TEST {name: 'a'}) return distinct [node in nodes(p) | properties(node)] as n, [link in relationships(p) | properties(link)] as r"
rs = graph.run(command)
path_set = rs.data()
rs.close()

# outside = ['h', 'i']
# target = 'a'
# data = []
# for item in outside:
#     exists = [item]
#     nodes[item]['children'] = test(nodes, links, item, target, exists)
#     data.append(nodes[item])
# print(data)

inside = defaultdict(int)
outside = {}
nodes = {}
links = {}
link_index = defaultdict(set)
for index, path in enumerate(path_set, 1):
    repeat = set()
    flag = False
    for item in path['n']:
        if item['id'] in repeat:
            flag = True
            break
        else:
            repeat.add(item['id'])
    if flag:
        continue

    outside[path['n'][0]['id']] = path['n'][0]
    nodes[path['n'][0]['id']] = path['n'][0]
    for item in path['n'][1:]:
        inside[item['id']] += 1
        nodes[item['id']] = item

    for link in path['r']:
        links[(link['from_id'], link['to_id'])] = link
        link_index[link['from_id']].add(link['to_id'])

    # print(index, [i['name'] for i in path['n']], len(path['n']) - 1)
else:
    target = path['n'][-1]


link_desc = defaultdict(set)
for key, value in link_index.items():
    link_desc[nodes[key]['name']] = [nodes[i]['name'] for i in value]

# print(target)
# print(outside)
print(nodes)
# print(link_index)
print(link_desc)
# print(links, len(links))

data = []
for key, value in outside.items():
    if key in inside:
        continue
    exists = []
    children, number = test(nodes, link_index, links, value, target, exists)
    nodes[key]['children'] = children
    nodes[key]['number'] = number

    # print(nodes[key])
    data.append(nodes[key])
    break
print(data)


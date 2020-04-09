import csv
import sys


ent_nodes = []
ent_nodes_set = set()
ent_links = []
ent_links_set = set()

per_nodes = []
per_nodes_set = set()
per_links = []
per_links_set = set()

bra_links = []
bra_links_set = set()


files = [
    ('qiyexinxi.csv', 'ent_nodes'),
    ('renyuanjiedian.csv', 'per_nodes'),
    ('ziranrentouzi.csv', 'per'),
    ('qiyetouzi.csv', 'ent_links'),
    ('qiyefenzhi.csv', 'bra'),
]

for file, label in files:
    if label == 'ent_nodes':
        with open(file) as f:
            reader = csv.reader(f)
            for row in f:
                ent_nodes.extend(row[-1])
                ent_nodes_set |= set(row[-1])
    elif label == 'per_nodes':
        with open(file) as f:
            reader = csv.reader(f)
            for row in f:
                per_nodes.extend(row[-1])
                per_nodes_set |= set(row[-1])
    elif label == 'per':
        with open(file) as f:
            reader = csv.reader(f)
            for row in f:
                per_links.extend(row[0])
                per_links_set |= set(row[0])
    elif label == 'ent_links':
        with open(file) as f:
            reader = csv.reader(f)
            for row in f:
                ent_links.append(row[0])
                ent_links.append(row[-1])
                ent_links_set.add(row[0])
                ent_links_set.add(row[-1])
    elif label == 'bra':
        with open(file) as f:
            reader = csv.reader(f)
            for row in f:
                bra_links.extend(row)
                bra_links_set |= set(row)

print(f'人员节点:{}\n人员节点(去重): {}\n企业节点:{}\n企业节点(去重): {}\n自然人投资:{}\n自然人投资(去重): {}\n企业投资:{}\n企业投资(去重): {}\n企业分支:{}\n企业分支(去重): {}')

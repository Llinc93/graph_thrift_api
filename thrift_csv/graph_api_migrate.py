# coding: utf8
import csv


ent_node_header = ['ID:ID(ENT-ID)', 'NAME', 'UNISCID', ':LABEL']
person_node_header = ['ID:ID(P-ID)', 'NAME', ':LABEL']

inv_relationship_header = ['ID:START_ID(P-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
ent_inv_relationship_header = ['ID:START_ID(ENT-ID)', 'RATE', 'ID:END_ID(ENT-ID)', ':TYPE']
bra_relationship_header = ['ID:END_ID(ENT-ID)', 'ID:START_ID(ENT-ID)', ':TYPE']
pos_relationship_header = ['ID:START_ID(P-ID)', 'POSITION', 'ID:END_ID(ENT-ID)', ':TYPE']

files = [
    (r'C:\Users\cpf\Desktop\CSV\ent_inv_relationship.csv', r'C:\Users\cpf\Desktop\ent_inv_relationship.csv', ent_inv_relationship_header, 'IPEE', '企业投资'),
    (r'C:\Users\cpf\Desktop\CSV\inv_relationship.csv', r'C:\Users\cpf\Desktop\inv_relationship.csv', inv_relationship_header, 'IPEE', '股东投资'),
    (r'C:\Users\cpf\Desktop\CSV\ent_node.csv', r'C:\Users\cpf\Desktop\ent_node.csv', ent_node_header, 'GS', '企业节点'),
    (r'C:\Users\cpf\Desktop\CSV\person_node.csv', r'C:\Users\cpf\Desktop\person_node.csv', person_node_header, 'GR', '人员节点'),
    (r'C:\Users\cpf\Desktop\CSV\bra_relationship.csv', r'C:\Users\cpf\Desktop\bra_relationship.csv', bra_relationship_header, 'BEE', '分支机构'),
    (r'C:\Users\cpf\Desktop\CSV\pos_relationship.txt', r'C:\Users\cpf\Desktop\pos_relationship.csv', pos_relationship_header, 'SPE', '人员任职'),
]


class WriteCSV(object):
    '''转化成neo4j需要的格式文件'''

    def GS(self, row):
        return [row[-3], row[5], row[-5], row[-1]]

    def GR(self, row):
        return [row[-2], row[0], row[-1]]

    def IPEE(self, row):
        return [row[-3], row[4], row[9] if row[9] else 0, row[-1]]

    def SPE(self, row):
        return [row[0], row[1], row[2], row[-1]]

    def BEE(self, row):
        return [row[1], row[-2], row[-1]]


def run():
    w_csv = WriteCSV()
    for read_file, write_file, header, label, desc in files:
        rf = open(read_file, 'r', encoding='utf8')
        wf = open(write_file, 'w', encoding='utf8', newline='')
        print(read_file)
        index = 1
        for row in csv.reader(rf):
            writer = csv.writer(wf)
            if index == 1:
                writer.writerow(header)
            else:
                new_row = getattr(w_csv, label)(row)
                writer.writerow([k if k else 'null' for k in new_row])
            index += 1

        rf.close()
        wf.close()
    return None

run()
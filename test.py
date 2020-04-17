import hashlib
from copy import deepcopy
from collections import defaultdict


def task(raw_data, params):

    s1 = time.time()
    ret = requests.get(url=config.EntRelevanceSeekGraphUrl, params=params)
    e1 = time.time()
    print('查询耗时', e1 - s1)
    raw_data = ret.json()
    data_nodes = []
    data_links = []
    nodes = {}
    links = defaultdict(list)
    pids = defaultdict(set)
    appear = {}
    null = {}
    start_node = raw_data['results'][0]['nodes'][0]
    end_node = None
    find_flag = False

    node_count = 0
    link_count = 1

    if raw_data['results'].pop()['@@res_flag']:
        while raw_data['results']:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            tmp_links = item['links']

            node_count += len(tmp_nodes)
            link_count += len(tmp_links)

            for node in tmp_nodes:
                if node['attributes']['name'] == params['ename']:
                    end_node = node
                    find_flag = True
                if find_flag:
                    if not node['attributes']['name']:
                        null.[node['v_id']] = 0
                        continue
                    if node['v_id'] in appear:
                        continue
                    appear.[node['v_id']] = 0
                    nodes[node['v_id']] = node

            if not find_flag:
                continue

            for link in tmp_links:
                if link['to_id'] in null or link['from_id'] in null:
                    continue
                pids[link['from_id']].add(link['to_id'])
                links[(link['from_id'], link['to_id'])].append(link)
        e2 = time.time()
        print('汇总耗时', e2 - e1)
        links_index = []
        stack = [start_node['v_id']]
        tmp_links = [[start_node['v_id']]]
        while stack:
            link = tmp_links.pop()
            tmp = stack.pop()
            if len(link) > params['level'] + 1:
                continue
            if tmp == end_node['v_id']:
                links_index.append(link)
                continue

            for pid in pids[tmp]:
                if pid in link:
                    continue
                action = deepcopy(link)
                action.append(pid)
                stack.append(pid)
                tmp_links.append(action)
        e3 = time.time()
        print('拼接耗时', e3 - e2)
        for link_index in links_index:
            for index in range(len(link_index) - 1):
                data_links.extend(links[(link_index[index], link_index[index + 1])])
                data_nodes.append(nodes[link_index[index]])
            data_nodes.append(nodes[link_index[-1]])
        print('构造耗时', time.time() - e3)
    return data_nodes, data_links, False


if __name__ == '__main__':
    data = {
        "version": {
            "edition": "developer",
            "api": "v2",
            "schema": 0
        },
        "error": False,
        "message": "",
        "results": [
            {
                "nodes": [
                    {
                        "v_id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司修理部",
                            "uniscid": "null",
                            "esdate": "1986-03-19",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "null",
                            "entstatus": "吊销，未注销",
                            "id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                            "@outdegree": 0
                        }
                    }
                ],
                "links": []
            },
            {
                "nodes": [
                    {
                        "v_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司",
                            "uniscid": "null",
                            "esdate": "1982-04-02",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "41",
                            "reccapcur": "人民币",
                            "entstatus": "吊销，未注销",
                            "id": "9a9298a5f35b71374ee86a9c6d9791b4",
                            "@outdegree": 5
                        }
                    }
                ],
                "links": [
                    {
                        "e_type": "REV_BEE",
                        "from_id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "v_id": "72edf3a59d69865f3aedcb974fb41fdf",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司智能游艺服务部",
                            "uniscid": "null",
                            "esdate": "1993-03-13",
                            "industry": "R",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "人民币",
                            "entstatus": "吊销，未注销",
                            "id": "72edf3a59d69865f3aedcb974fb41fdf",
                            "@outdegree": 1
                        }
                    },
                    {
                        "v_id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司修理部",
                            "uniscid": "null",
                            "esdate": "1986-03-19",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "null",
                            "entstatus": "吊销，未注销",
                            "id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                            "@outdegree": 1
                        }
                    },
                    {
                        "v_id": "332d1a9bcac6f0ba67b2f71e0931f019",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司经营部",
                            "uniscid": "null",
                            "esdate": "1987-01-24",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "人民币",
                            "entstatus": "注销",
                            "id": "332d1a9bcac6f0ba67b2f71e0931f019",
                            "@outdegree": 1
                        }
                    },
                    {
                        "v_id": "0a1d273b377acb6b2a6b133ac3a26d08",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司门市部",
                            "uniscid": "null",
                            "esdate": "1986-03-19",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "人民币",
                            "entstatus": "吊销，未注销",
                            "id": "0a1d273b377acb6b2a6b133ac3a26d08",
                            "@outdegree": 1
                        }
                    },
                    {
                        "v_id": "88341ca058be014e108621327dd559ff",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司老菜饭庄",
                            "uniscid": "null",
                            "esdate": "1994-02-22",
                            "industry": "H",
                            "province": "320000",
                            "regcap": "0",
                            "reccapcur": "人民币",
                            "entstatus": "注销",
                            "id": "88341ca058be014e108621327dd559ff",
                            "@outdegree": 1
                        }
                    }
                ],
                "links": [
                    {
                        "e_type": "BEE",
                        "from_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "from_type": "GS",
                        "to_id": "72edf3a59d69865f3aedcb974fb41fdf",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "BEE",
                        "from_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "from_type": "GS",
                        "to_id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "BEE",
                        "from_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "from_type": "GS",
                        "to_id": "332d1a9bcac6f0ba67b2f71e0931f019",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "BEE",
                        "from_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "from_type": "GS",
                        "to_id": "0a1d273b377acb6b2a6b133ac3a26d08",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "BEE",
                        "from_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "from_type": "GS",
                        "to_id": "88341ca058be014e108621327dd559ff",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "v_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "v_type": "GS",
                        "attributes": {
                            "name": "镇江市广播电视服务公司",
                            "uniscid": "null",
                            "esdate": "1982-04-02",
                            "industry": "F",
                            "province": "320000",
                            "regcap": "41",
                            "reccapcur": "人民币",
                            "entstatus": "吊销，未注销",
                            "id": "9a9298a5f35b71374ee86a9c6d9791b4",
                            "@outdegree": 5
                        }
                    }
                ],
                "links": [
                    {
                        "e_type": "REV_BEE",
                        "from_id": "72edf3a59d69865f3aedcb974fb41fdf",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "REV_BEE",
                        "from_id": "8be5e8b4e2ed29cab6a0bf550a2c2baf",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "REV_BEE",
                        "from_id": "332d1a9bcac6f0ba67b2f71e0931f019",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "REV_BEE",
                        "from_id": "0a1d273b377acb6b2a6b133ac3a26d08",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    },
                    {
                        "e_type": "REV_BEE",
                        "from_id": "88341ca058be014e108621327dd559ff",
                        "from_type": "GS",
                        "to_id": "9a9298a5f35b71374ee86a9c6d9791b4",
                        "to_type": "GS",
                        "directed": True,
                        "attributes": {
                            "rate": 1
                        }
                    }
                ]
            },
            {
                "@@res_flag": True
            }
        ]
    }
    params = {
        'sname': '镇江市广播电视服务公司修理部',
        'ename': '镇江市广播电视服务公司'
    }
    nodes, links, flag = task(raw_data=data, params=params)
    # for link in links:
        # print(link)
    print()
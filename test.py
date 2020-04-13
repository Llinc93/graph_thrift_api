def task(raw_data, params):
    nodes = []
    links = []
    null = []
    path = set()
    if raw_data['results'].pop()['@@res_flag']:
        snode = raw_data['results'][0]['nodes'][0]
        while raw_data['results']:
            item = raw_data['results'].pop()
            tmp_nodes = item['nodes']
            tmp_links = item['links']

            for node in tmp_nodes:
                if not node['attributes']['name']:
                    null.append(node['v_id'])
                    continue

                if node['attributes']['name'] == params['ename']:
                    path.add(node['v_id'])
                    nodes.append(node)
                    flag = False
                elif node['v_id'] in path:
                    nodes.append(node)

            for link in tmp_links:
                if link['to_id'] in null or link['from_id'] in null:
                    continue

                if link['to_id'] in path and link['to_id'] != snode['v_id']:
                    path.add(link['from_id'])
                    links.append(link)
    return nodes, links, False


if __name__ == '__main__':
    data = [

    ]
    nodes, links, flag = task(raw_data=data)
    print(nodes)
    print(links)
    print()
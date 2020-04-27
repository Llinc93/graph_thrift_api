data = \
{
    "nodes": [
        {
            "id": "493cd250bdfbb7f821ff45664983ee82",
            "name": "宜兴市天健医药连锁有限公司",
            "type": "GS",
            "attibuteMap": {
                "extendNumber": 181,
                "industry_class": "F",
                "business_age": "2003",
                "province": "320000",
                "regcapcur": "人民币",
                "registered_capital": "1698",
                "business_status": "在营（开业）企业"
            }
        },
        {
            "id": "37abb2e79d70a2e599f471f30b69772e",
            "name": "关艳与朱倩虹、中国人民财产保险股份有限公司无锡市分公司机动车交通事故责任纠纷一审民事判决书",
            "type": "LL",
            "attibuteMap": {
                "extendNumber": 2
            }
        },
        {
            "id": "1b9d4012de039d042e9196e756b19ccb",
            "name": "中国人民财产保险股份有限公司无锡市分公司",
            "type": "GS",
            "attibuteMap": {
                "extendNumber": 3112,
                "industry_class": "J",
                "business_age": "1996",
                "province": "320000",
                "regcapcur": "人民币",
                "registered_capital": "0",
                "business_status": "在营（开业）企业"
            }
        },
        {
            "id": "a83b17ac9954f5ce538e60ecd5e4ab84",
            "name": "叶礼方与王建伟、中国人民财产保险股份有限公司无锡市分公司机动车交通事故责任纠纷一审民事判决书",
            "type": "LL",
            "attibuteMap": {
                "extendNumber": 0
            }
        },
        {
            "id": "715b89cdfed5f7194b0e2e448fb2bca9",
            "name": "无锡市天瑞园艺有限公司",
            "type": "GS",
            "attibuteMap": {
                "extendNumber": 4,
                "industry_class": "N",
                "business_age": "2002",
                "province": "320000",
                "regcapcur": "人民币",
                "registered_capital": "50",
                "business_status": "在营（开业）企业"
            }
        },
        {
            "id": "0c313ee5ae8bf2d2570886d30e54f730",
            "name": "陆罗柯",
            "type": "GR",
            "attibuteMap": {
                "extendNumber": 1
            }
        },
        {
            "id": "2d83ca03d709f922390bf236107b5ad7",
            "name": "无锡市镜厂",
            "type": "GS",
            "attibuteMap": {
                "extendNumber": 0,
                "industry_class": "C",
                "business_age": "1980",
                "province": "320000",
                "regcapcur": "人民币",
                "registered_capital": "44",
                "business_status": "吊销，未注销"
            }
        }
    ],
    "success": 0,
    "links": [
        {
            "id": "903e5e2c8632021c247cd745bc9a770e",
            "name": "诉讼",
            "from": "493cd250bdfbb7f821ff45664983ee82",
            "to": "37abb2e79d70a2e599f471f30b69772e",
            "type": "LEL",
            "attibuteMap": {}
        },
        {
            "id": "1cf3d46d0516ba69437f288b58739cd4",
            "name": "诉讼",
            "from": "1b9d4012de039d042e9196e756b19ccb",
            "to": "37abb2e79d70a2e599f471f30b69772e",
            "type": "LEL",
            "attibuteMap": {}
        },
        {
            "id": "a325773e315766a9d86f1ad431cbeff6",
            "name": "诉讼",
            "from": "1b9d4012de039d042e9196e756b19ccb",
            "to": "a83b17ac9954f5ce538e60ecd5e4ab84",
            "type": "LEL",
            "attibuteMap": {}
        },
        {
            "id": "0d8fd50e032f3f02460f58922434cd2d",
            "name": "诉讼",
            "from": "715b89cdfed5f7194b0e2e448fb2bca9",
            "to": "a83b17ac9954f5ce538e60ecd5e4ab84",
            "type": "LEL",
            "attibuteMap": {}
        },
        {
            "id": "416cc2c6f6e76ee40e3b74100dafc8ae",
            "name": "任职",
            "from": "0c313ee5ae8bf2d2570886d30e54f730",
            "to": "715b89cdfed5f7194b0e2e448fb2bca9",
            "type": "SPE",
            "attibuteMap": {
                "position": "监事"
            }
        },
        {
            "id": "d35943ae95333e4fd1a84bfee67120b0",
            "name": "投资",
            "from": "0c313ee5ae8bf2d2570886d30e54f730",
            "to": "2d83ca03d709f922390bf236107b5ad7",
            "type": "IPEE",
            "attibuteMap": {
                "conratio": 0.00227,
                "holding_mode": "参股"
            }
        }
    ]
}

nodes = data['nodes']
links = data['links']

ids = {}
tmp_nodes = {}

for node in nodes:
    ids[node['id']] = 0
    tmp_nodes[node['id']] = node

for link in links:
    ids[link['from']] += 1
    ids[link['to']] += 1

for key, value in ids.items():
    print(key, tmp_nodes[key]['name'], value)

import json
import traceback
from flask import Blueprint, request

from model.ent_graph import neo4j_client
from parse.parse_graph import parse


MOD = Blueprint('mod', __name__)


@MOD.route('/getFinalBeneficiaryName', methods=['POST'])
def getFinalBeneficiaryName():
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        entName = request.form.get('entName')
        uscCode = request.form.get('uscCode')
        min_ratio = request.form.get('min_ratio', 0)
        if not min_ratio:
            min_ratio = 0
        lcid = neo4j_client.get_lcid(entname=entName, usccode=uscCode)
        if not lcid:
            return json.dumps({'data': [], 'success': 0}, ensure_ascii=False)
        level = neo4j_client.get_level(lcid=lcid)
        data = neo4j_client.get_final_beneficiary_name(entname=entName, usccode=uscCode, level=level)
        if not data:
            return json.dumps({'data': [], 'success': 0}, ensure_ascii=False)
        data = parse.get_final_beneficiary_name(data, min_rate=min_ratio, lcid=lcid)

        return json.dumps({'data': data, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        # print('error')
        return json.dumps({'data': '', 'success': 104}, ensure_ascii=False)

# def getFinalBeneficiaryName_ti(self, entName, uscCode, min_ratio=0):
#     """
#     企业实际控股人信息
#     entName 企业名称
#
#     Parameters:
#      - entName
#     """
#     try:
#         from model import tiger_graph
#         from parse import tiger_graph_parse
#
#         entName = request.form.get('entName')
#         uscCode = request.form.get('uscCode')
#         min_ratio = request.form.get('min_ratio')
#
#         raw_data = tiger_graph.get_ent_actual_controller(name=entName, uniscid=uscCode)
#
#         if raw_data['error']:
#             raise ValueError
#
#         data = tiger_graph_parse.get_final_beneficiary_name(raw_data, min_ratio, entName)
#         return json.dumps({'data': data, 'success': 0}, ensure_ascii=False)
#     except:
#         traceback.print_exc()
#         # print('error')
#         return json.dumps({'data': '', 'success': 104}, ensure_ascii=False)

@MOD.route('/getEntActualContoller', methods=['POST'])
def getEntActualContoller():
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        entName = request.form.get('entName')
        uscCode = request.form.get('uscCode')
        min_ratio = float(request.form.get('min_ratio', 0))

        if not min_ratio:
            min_ratio = 0
        lcid = neo4j_client.get_lcid(entname=entName, usccode=uscCode)
        if not lcid:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        level = neo4j_client.get_level(lcid=lcid)
        data = neo4j_client.get_ent_actual_controller(entname=entName, usccode=uscCode, level=level)
        if not data:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        nodes, links = parse.get_ent_actual_controller(data, min_rate=min_ratio)
        # print(time.time() - start)
        if not links:
            nodes = []
        return json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        # print('error')
        return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)

# def getEntActualContoller_tiger(self, entName, uscCode, min_ratio=0):
#     """
#     企业实际控股人信息
#     entName 企业名称
#
#     Parameters:
#      - entName
#     """
#     try:
#         from model import tiger_graph
#         from parse import tiger_graph_parse
#
#         raw_data = tiger_graph.get_ent_actual_controller(name=entName, uniscid=uscCode)
#
#         if raw_data['error']:
#             raise ValueError
#
#         nodes, links = tiger_graph_parse.ent_actual_controller(raw_data, min_ratio)
#         return json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
#     except:
#         traceback.print_exc()
#         # print('error')
#         return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)

@MOD.route('/getEntGraphG', methods=['POST'])
def getEntGraphG():
    """
    企业图谱查询
    keyword 关键字
    attIds 过滤关系
    level 层级，最大3层
    nodeType 节点类型

    Parameters:
     - keyword
     - attIds
     - level
     - nodeType
    """
    try:
        keyword = request.form['keyword']
        attIds = request.form['attIds']
        level = int(request.form['level'])
        nodeType = float(request.form['nodeType'])

        if int(level) > 3 or int(level) <= 0:
            raise ValueError

        relationshipFilter = parse.get_relationshipFilter(attIds)
        if not relationshipFilter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        data, flag = neo4j_client.get_ent_graph_g_v4(keyword, int(level) + 1, nodeType, relationshipFilter)

        if not flag:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        # nodes, links = parse.parse_v3(data, filter, int(level), keyword)
        nodes, links = parse.parse_v5(data, int(level))
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)

# def getEntGraphG_ti(self, keyword, attIds, level, nodeType):
#     """
#     企业图谱查询
#     keyword 关键字
#     attIds 过滤关系
#     level 层级，最大3层
#     nodeType 节点类型
#
#     Parameters:
#      - keyword
#      - attIds
#      - level
#      - nodeType
#     """
#     try:
#         from model import tiger_graph
#         from parse import tiger_graph_parse
#
#         raw_data = tiger_graph.get_ent_graph(name=keyword, node_type=nodeType, level=level, attIds=attIds)
#
#         if raw_data['error']:
#             raise ValueError
#
#         nodes, links = tiger_graph_parse.ent_graph(raw_data)
#
#         return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
#     except:
#         traceback.print_exc()
#         return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)

@MOD.route('/getEntsRelevanceSeekGraphG', methods=['POST'])
def getEntsRelevanceSeekGraphG():
    """
    企业关联探寻
    entName 企业名称
    attIds 过滤关系
    level 层级，最大6层

    Parameters:
     - entName
     - attIds
     - level
    """
    try:
        entName = request.form['entName']
        attIds = request.form['attIds']
        level = int(request.form['level'])

        relationshipFilter = parse.get_relationshipFilter(attIds)
        if not relationshipFilter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.parallel_query(entName, int(level), relationshipFilter)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)

# def getEntsRelevanceSeekGraphG_ti(self, entName, attIds, level):
#     """
#     企业关联探寻
#     entName 企业名称
#     attIds 过滤关系
#     level 层级，最大6层
#
#     Parameters:
#      - entName
#      - attIds
#      - level
#     """
#     try:
#         from model import tiger_graph
#         from parse import tiger_graph_parse
#
#         raw_data = tiger_graph.get_ent_relevance_seek_graph(names=entName, attIds=attIds, level=level)
#         nodes, links = tiger_graph_parse.ent_relevance_seek_graph(raw_data)
#         return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
#     except:
#         traceback.print_exc()
#         return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


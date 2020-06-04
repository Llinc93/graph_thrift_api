import json
import time
import hashlib
import traceback
from flask import Blueprint, request, send_file

from model.ent_graph import neo4j_client
# from model.ent_graph import RedisClient
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
        min_ratio = float(request.form.get('min_ratio', 0))
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
        return json.dumps({'data': '', 'success': 104}, ensure_ascii=False)


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

        start = time.time()
        if not min_ratio:
            min_ratio = 0
        lcid = neo4j_client.get_lcid(entname=entName, usccode=uscCode)
        if not lcid:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)

        # redis_client = RedisClient()

        # 查找缓存
        # name = hashlib.md5(f'getEntActualContoller,{lcid},{min_ratio}'.encode('utf8'))
        # if redis_client.r.exists(name):
        #     cache = redis_client.r.get(name)
        #     return cache

        level = neo4j_client.get_level(lcid=lcid)
        data = neo4j_client.get_ent_actual_controller(entname=entName, usccode=uscCode, level=level)
        if not data:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        nodes, links = parse.get_ent_actual_controller(data, min_rate=min_ratio)
        if not links:
            nodes = []
        end = time.time()
        res = json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
        print(f'getEntActualContoller: {end - start}s')
        # if end - start > 10:
        #     redis_client.r.set(name, res)
        return res
     except:
        traceback.print_exc()
        return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)


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
        nodeType = request.form['nodeType']

        start = time.time()
        if level> 3 or level <= 0:
            raise ValueError

        # 查找缓存
        # redis_client = RedisClient()
        # name = hashlib.md5(f'getEntGraphG,{keyword},{attIds},{level},{nodeType}'.encode('utf8'))
        # if redis_client.r.exists(name):
        #     cache = redis_client.r.get(name)
        #     return cache

        relationshipFilter = parse.get_relationshipFilter(attIds)
        if not relationshipFilter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        data, flag = neo4j_client.get_ent_graph_g(keyword, level + 1, nodeType, relationshipFilter)

        if not flag:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.ent_graph_parse(data, level, relationshipFilter)
        end = time.time()
        res = json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
        print(f'getEntGraphG: {end -start}s')
        # if ent - start > 10:
        #     redis_client.r.set(name, res)
        return res
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)


# @MOD.route('/getEntsRelevanceSeekGraphG', methods=['POST'])
# def getEntsRelevanceSeekGraphG():
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
#         entName = request.form['entName']
#         attIds = request.form['attIds']
#         level = int(request.form['level'])
#
#         start = time.time()
#         # 查找缓存
#         sort_name = ';'.join(sorted(entName.split(';')))
#         # redis_client = RedisClient()
#         # name = hashlib.md5(f'getEntsRelevanceSeekGraphG,{sort_name},{attIds},{level}'.encode('utf8'))
#         # if redis_client.r.exists(name):
#         #     cache = redis_client.r.get(name)
#         #     return cache
#
#         relationshipFilter = parse.get_relationshipFilter(attIds)
#         if not relationshipFilter:
#             return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)
#
#         # nodes, links = parse.parallel_query(entName, int(level), relationshipFilter)
#         entName = sorted(entName.split(';'))
#         nodes, links = parse.ent_relevance_seek_graph(entName, int(level), relationshipFilter)
#         end = time.time()
#         res = json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
#         print(f'getEntsRelevanceSeekGraphG: {end - start}s')
#         # if ent - start > 10:
#         #     redis_client.r.set(name, res)
#         return res
#     except json.JSONDecodeError:
#         return json.dumps({'nodes': [], 'success': 103, 'links': [], 'msg': '数据量超出限制，请缩小查询条件'}, ensure_ascii=False)
#     except:
#         traceback.print_exc()
#         return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


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

        start = time.time()
        # 查找缓存
        names = sorted(entName.split(';'))
        sort_name = ';'.join(names)
        # redis_client = RedisClient()
        # name = hashlib.md5(f'getEntsRelevanceSeekGraphG,{sort_name},{attIds},{level}'.encode('utf8'))
        # if redis_client.r.exists(name):
        #     cache = redis_client.r.get(name)
        #     return cache

        relationshipFilter = parse.get_relationshipFilter(attIds)
        if not relationshipFilter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        data, flag = neo4j_client.get_ent_relevance_seek_graph(names, level, relationshipFilter)

        if not flag:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.ent_relevance_seek_graph(data, int(level), relationshipFilter)

        end = time.time()
        res = json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
        print(f'getEntsRelevanceSeekGraphG: {end - start}s')
        # if ent - start > 10:
        #     redis_client.r.set(name, res)
        return res
    except json.JSONDecodeError:
        return json.dumps({'nodes': [], 'success': 103, 'links': [], 'msg': '数据量超出限制，请缩小查询条件'}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)
import json
import time
import traceback
from flask import Blueprint, request

from model import tiger_graph
from parse import tiger_graph_parse


MOD = Blueprint('mod', __name__)


@MOD.route('/getFinalBeneficiaryName', methods=['POST'])
def get_final_beneficiary_name():
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        s = time.time()
        entName = request.form.get('entName')
        uscCode = request.form.get('uscCode')
        min_ratio = float(request.form.get('min_ratio', 0))
        redis_client = tiger_graph.RedisClient()
        lcid = redis_client.r.get(entName)

        #raw_data = tiger_graph.get_ent_actual_controller(name=entName, uniscid=uscCode)
        raw_data = tiger_graph.get_final_beneficiary_name(name=lcid, uniscid=uscCode)
        e = time.time()
        print('查询耗时', e - s)
        if raw_data['error']:
            raise ValueError

        # data = tiger_graph_parse.get_final_beneficiary_name(raw_data, min_ratio, entName)
        data = tiger_graph_parse.get_final_beneficiary_name_v2(raw_data, min_ratio)
        print('总耗时', time.time() - s)
        return json.dumps({'data': data, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'data': '', 'success': 104}, ensure_ascii=False)


@MOD.route('/getEntActualContoller', methods=['POST'])
def get_ent_actual_contoller():
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        s = time.time()
        entName = request.form.get('entName')
        uscCode = request.form.get('uscCode')
        min_ratio = float(request.form.get('min_ratio', 0))

        redis_client = tiger_graph.RedisClient()
        lcid = redis_client.r.get(entName)

        raw_data = tiger_graph.get_ent_actual_controller(name=lcid, uniscid=uscCode)
        e = time.time()
        print('查询耗时', e - s)
        if raw_data['error']:
            raise ValueError

        nodes, links = tiger_graph_parse.ent_actual_controller(entName, raw_data, min_ratio)
        print('构造耗时', time.time() - e)
        print('总耗时', time.time() - s)
        return json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        # print('error')
        return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)


@MOD.route('/getEntGraphG', methods=['POST'])
def get_ent_graph_g():
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
        s = time.time()
        keyword = request.form['keyword']
        attIds = request.form['attIds']
        level = int(request.form['level'])
        nodeType = request.form['nodeType']

        raw_data = tiger_graph.get_ent_graph(name=keyword, node_type=nodeType, level=level, attIds=attIds)
        e = time.time()
        print('查询耗时', e - s)
        if raw_data['error']:
            raise ValueError

        nodes, links = tiger_graph_parse.ent_graph(raw_data)
        print('构造耗时', time.time() - e)
        print('总耗时', time.time() - s)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)


@MOD.route('/getEntsRelevanceSeekGraphG', methods=['POST'])
def get_ents_relevance_seek_graph_g():
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
        s = time.time()
        entName = request.form['entName']
        attIds = request.form['attIds']
        level = int(request.form['level'])

        raw_data = tiger_graph.get_ent_relevance_seek_graph(names=entName, attIds=attIds, level=level)
        nodes, links = tiger_graph_parse.ent_relevance_seek_graph(raw_data)
        print('总耗时', time.time() - s)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


@MOD.route('/test', methods=['POST'])
def test():
    try:
        s = time.time()
        entName = request.form['entName']
        attIds = request.form['attIds']
        level = int(request.form['level'])

        raw_data = tiger_graph.get_ent_relevance_seek_graph_v2(names=entName, attIds=attIds, level=level)
        nodes, links = tiger_graph_parse.ent_relevance_seek_graph(raw_data)
        print('test总耗时', time.time() - s)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except json.JSONDecodeError:
        return json.dumps({'nodes': [], 'success': 103, 'links': [], 'msg': '数据量超出限制，请缩小查询条件'}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)

import json
import time
import hashlib
import traceback
from flask import Blueprint, request, current_app

from config import CACHE_FLAG
from model.ent_graph import neo4j_client
from model.ent_graph import redis_client
from parse.parse_graph import parse

MOD = Blueprint('mod', __name__)


@MOD.route('/getEntActualContoller', methods=['POST'])
def get_ent_actual_controller():
    """
    企业实际控股人信息
    :return:
    """
    try:
        start = time.time()
        ent_name = request.form.get('entName')
        usc_code = request.form.get('uscCode')
        min_ratio = float(request.form.get('min_ratio', 0))

        if not min_ratio:
            min_ratio = 0
        lcid = neo4j_client.get_lcid(entname=ent_name, usccode=usc_code)
        if not lcid:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)

        # 查找缓存
        name = hashlib.md5(f'getEntActualContoller,{lcid},{min_ratio}'.encode('utf8')).hexdigest()
        if CACHE_FLAG and redis_client.r.exists(name):
            cache = redis_client.r.get(name)
            return cache

        level = neo4j_client.get_level(lcid=lcid)
        data = neo4j_client.get_ent_actual_controller(entname=ent_name, usccode=usc_code, level=level)
        if not data:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        nodes, links = parse.get_ent_actual_controller(data, min_rate=min_ratio)
        if not links:
            nodes = []
        res = json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)

        end = time.time()
        # print(f'getEntActualContoller: {end - start}s')
        if end - start > 10 and CACHE_FLAG:
            redis_client.r.set(name, res)
        return res
    except Exception:
        exc = traceback.format_exc()
        current_app.logger.error(f'getEntActualContoller:\t{exc}')
        return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)


@MOD.route('/getEntGraphG', methods=['POST'])
def get_ent_graph_g():
    """
    企业图谱
    :return:
    """
    try:
        keyword = request.form['keyword']
        att_ids = request.form['attIds']
        level = int(request.form['level'])
        node_type = request.form['nodeType']

        start = time.time()
        if level > 3 or level <= 0:
            raise ValueError

        # 查找缓存
        name = hashlib.md5(f'getEntGraphG,{keyword},{att_ids},{level},{node_type}'.encode('utf8')).hexdigest()
        if CACHE_FLAG and redis_client.r.exists(name):
            cache = redis_client.r.get(name)
            return cache

        relationship_filter = parse.get_relationship_filter(att_ids)
        if not relationship_filter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        data, flag = neo4j_client.get_ent_graph_g(keyword, level + 1, node_type, relationship_filter)
        if not flag:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.ent_graph_parse(data, level)
        res = json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)

        end = time.time()
        # print(f'getEntGraphG: {end -start}s')
        if end - start > 10 and CACHE_FLAG:
            redis_client.r.set(name, res)
        return res
    except Exception:
        exc = traceback.format_exc()
        current_app.logger.error(f'getEntGraphG:\t{exc}')
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
        start = time.time()

        ent_name = request.form['entName']
        att_ids = request.form['attIds']
        level = int(request.form['level'])

        names = ent_name.split(';')

        # 查找缓存
        sort_name = ';'.join(sorted(names))
        name = hashlib.md5(f'getEntsRelevanceSeekGraphG,{sort_name},{att_ids},{level}'.encode('utf8')).hexdigest()
        if CACHE_FLAG and redis_client.r.exists(name):
            cache = redis_client.r.get(name)
            return cache

        relationship_filter = parse.get_relationship_filter(att_ids)
        if not relationship_filter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        data, flag = neo4j_client.get_ent_relevance_seek_graph(names, level, relationship_filter)
        if not flag:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.ent_relevance_seek_graph(data, int(level), relationship_filter)
        res = json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)

        end = time.time()
        # print(f'getEntsRelevanceSeekGraphG: {end - start}s')
        if end - start > 10 and CACHE_FLAG:
            redis_client.r.set(name, res)
        return res
    except json.JSONDecodeError:
        exc = traceback.format_exc()
        current_app.logger.warning(f'getEntsRelevanceSeekGraphG:\t{exc}')
        return json.dumps({'nodes': [], 'success': 103, 'links': [], 'msg': '数据量超出限制，请缩小查询条件'}, ensure_ascii=False)
    except Exception:
        exc = traceback.format_exc()
        current_app.logger.error(exc)
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)

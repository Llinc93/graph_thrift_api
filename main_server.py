# -*- coding: UTF-8 -*-
"""
thrift服务端
"""
import traceback, os, sys, json, time, redis
if sys.platform.startswith('win'):
    sys.path.append( os.getcwd() + '\com\\thrift\interface\server')
    sys.path.append( os.getcwd() + '\com\\thrift')
else:
    sys.path.append( os.getcwd() + '/com/thrift/interface/server')
    sys.path.append( os.getcwd() + '/com/thrift')

from thrift.transport import TSocket, TTransport
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.server import TServer, TProcessPoolServer

from interface.server import Interface
from interface.server.ttypes import AuditException
from model.ent_graph import neo4j_client
from parse.parse_graph import parse


class MyFaceHandler(Interface.Iface):

  def __init__(self):
    pass

  def getEntActualContoller(self, entName, uscCode, min_ratio=0):
    """
    企业实际控制人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        # start = time.time()
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
        return json.dumps({'data':'', 'success':101}, ensure_ascii=False)

  def getEntGraphG(self, keyword, attIds, level, nodeType):
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
        # # start = time.time()
        # terms = parse.get_term(attIds.split(';'))
        # data = neo4j_client.get_ent_graph_g(entname=keyword, level=level, node_type=nodeType, terms=terms)
        # if not data:
        #     return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        # nodes, links = parse.parse(data)
        # # print(time.time() - start)
        # return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
        terms = parse.get_term_v2(attIds.split(';'))
        data, flag = neo4j_client.get_ent_graph_g_v2(entname=keyword, level=level, node_type=nodeType, terms=terms)
        if not flag:
            return json.dumps({'nodes': [data], 'success': 0, 'links': []}, ensure_ascii=False)
        nodes, links = parse.parse_v2(data)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)

  def getEntsRelevanceSeekGraphG(self, entName, attIds, level):
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
        if not attIds or not entName:
            return {'nodes': [], 'success': 0, 'links': []}
        # start = time.time()
        terms = parse.get_term(attIds.split(';'))
        data = neo4j_client.get_ents_relevance_seek_graph_g(entnames=entName.split(';'), level=level, terms=terms)
        if not data:
            return json.dumps({'data': {'nodes': [], 'links': []}, 'success': 0}, ensure_ascii=False)
        nodes, links = parse.parse(data)
        # print(time.time() - start)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.format_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


if __name__ == '__main__':

    handler = MyFaceHandler()
    processor = Interface.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=9918)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    # pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TProcessPoolServer.TProcessPoolServer(processor, transport, tfactory, pfactory)
    server.setNumWorkers(os.cpu_count())
    server.serve()


    rpc_server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    rpc_server.serve()
    print('close done')


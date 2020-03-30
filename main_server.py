# -*- coding: UTF-8 -*-
"""
thrift服务端
"""
import traceback, os, sys, json

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

  # def getFinalBeneficiaryName(self, entName, uscCode, min_ratio=0):
  def getEntActualContoller_v1(self, entName, uscCode, min_ratio=0):
      """
      企业实际控股人信息
      entName 企业名称

      Parameters:
       - entName
      """
      try:
          if not min_ratio:
              min_ratio = 0
          lcid = neo4j_client.get_lcid(entname=entName, usccode=uscCode)
          if not lcid:
              return json.dumps({'data': [], 'success': 0}, ensure_ascii=False)
          level = neo4j_client.get_level(lcid=lcid)
          data = neo4j_client.get_final_beneficiary_name()(entname=entName, usccode=uscCode, level=level)
          if not data:
              return json.dumps({'data': [], 'success': 0}, ensure_ascii=False)
          data = parse.get_final_beneficiary_name(data, min_rate=min_ratio, lcid=lcid)

          return json.dumps({'data': data, 'success': 0}, ensure_ascii=False)
      except:
          traceback.print_exc()
          # print('error')
          return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)

  def getEntActualContoller_v1(self, entName, uscCode, min_ratio=0):
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
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

  def getEntActualContoller_tiger(self, entName, uscCode, min_ratio=0):
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        from model import tiger_graph
        from parse import tiger_graph_parse

        raw_data = tiger_graph.get_ent_actual_controller(name=entName, uniscid=uscCode)

        if raw_data['error']:
            raise ValueError

        nodes, links = tiger_graph_parse.ent_actual_controller(raw_data, min_ratio)
        return json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        # print('error')
        return json.dumps({'data':'', 'success':101}, ensure_ascii=False)

  def getEntGraphG_v1(self, keyword, attIds, level, nodeType):
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
        from model import tiger_graph
        from parse import tiger_graph_parse

        raw_data = tiger_graph.get_ent_graph(name=keyword, node_type=nodeType, level=level, attIds=attIds)

        if raw_data['error']:
            raise ValueError

        nodes, links = tiger_graph_parse.ent_graph(raw_data)

        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 102, 'links': []}, ensure_ascii=False)

  def getEntsRelevanceSeekGraphG_v1(self, entName, attIds, level):
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
        if int(level) <= 0 or int(level) > 6:
            raise ValueError

        nodes, links, filter, direct = parse.get_term_v3(attIds.split(';'))
        if not filter:
            return json.dumps({'nodes': [], 'success': 0, 'links': []}, ensure_ascii=False)

        nodes, links = parse.parallel_query(entName, level, nodes, links, filter, direct)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


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
        from model import tiger_graph
        from parse import tiger_graph_parse

        raw_data = tiger_graph.get_ent_relevance_seek_graph(names=entName, attIds=attIds, level=level)
        nodes, links = tiger_graph_parse.ent_relevance_seek_graph(raw_data)
        return json.dumps({'nodes': nodes, 'success': 0, 'links': links}, ensure_ascii=False)
    except:
        traceback.print_exc()
        return json.dumps({'nodes': [], 'success': 103, 'links': []}, ensure_ascii=False)


if __name__ == '__main__':
    handler = MyFaceHandler()
    processor = Interface.Processor(handler)
    transport = TSocket.TServerSocket(host='0.0.0.0', port=19918)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    # pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TProcessPoolServer.TProcessPoolServer(processor, transport, tfactory, pfactory)
    server.setNumWorkers(os.cpu_count())
    server.serve()
                                                                        
    rpc_server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    rpc_server.serve()
    print('close done')


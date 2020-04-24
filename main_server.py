# -*- coding: UTF-8 -*-
"""
thrift服务端
"""
import traceback, os, sys, json, time

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

from model import tiger_graph
from parse import tiger_graph_parse

class MyFaceHandler(Interface.Iface):

  def __init__(self):
    pass

  def getFinalBeneficiaryName(self, entName, uscCode, min_ratio=0):
      """
      企业实际控股人信息
      entName 企业名称

      Parameters:
       - entName
      """
      try:
          s = time.time()
          raw_data = tiger_graph.get_final_beneficiary_name(name=entName, uniscid=uscCode)
          e = time.time()
          print('查询耗时', e - s)
          if raw_data['error']:
              raise ValueError

          # data = tiger_graph_parse.get_final_beneficiary_name(raw_data, min_ratio, entName)
          data = tiger_graph_parse.get_final_beneficiary_name_v2(raw_data, float(min_ratio), entName)
          print('总耗时', time.time() - s)
          return json.dumps({'data': data, 'success': 0}, ensure_ascii=False)
      except:
          traceback.print_exc()
          # print('error')
          return json.dumps({'data': '', 'success': 104}, ensure_ascii=False)

  def getEntActualContoller(self, entName, uscCode, min_ratio=0):
    """
    企业实际控股人信息
    entName 企业名称

    Parameters:
     - entName
    """
    try:
        s = time.time()

        raw_data = tiger_graph.get_ent_actual_controller(name=entName, uniscid=uscCode)
        e = time.time()
        print('查询耗时', e - s)
        if raw_data['error']:
            raise ValueError

        nodes, links = tiger_graph_parse.ent_actual_controller(entName, raw_data, float(min_ratio))
        print('构造耗时', time.time() - e)
        print('总耗时', time.time() - s)
        return json.dumps({'data': {'nodes': nodes, 'links': links}, 'success': 0}, ensure_ascii=False)
    except:
        traceback.print_exc()
        # print('error')
        return json.dumps({'data': '', 'success': 101}, ensure_ascii=False)

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
    transport = TSocket.TServerSocket(host='0.0.0.0', port=8140)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    # pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TProcessPoolServer.TProcessPoolServer(processor, transport, tfactory, pfactory)
    server.setNumWorkers(os.cpu_count())
    server.serve()
                                                                        
    rpc_server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    rpc_server.serve()
    print('close done')


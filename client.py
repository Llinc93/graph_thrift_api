import traceback

from com.thrift.interface.server import Interface

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


class PyClient():

    def getEntActualContoller(self, username, uscCode, mix_rate):
        try:
            # 建立socket
            transport = TSocket.TSocket('', 9011)
            # 选择传输协议，和服务端一致
            transport = TTransport.TBufferedTransport(transport)
            # protocol = TBinaryProtocol.TBinaryProtocol(transport)
            protocol = TCompactProtocol.TCompactProtocol(transport)
            # 创建客户端
            client = Interface.Client(protocol)
            transport.open()
            data = client.getEntActualContoller(username, uscCode, mix_rate)
            print(data)
            transport.close()
            return data
        # 捕获异常
        except:
            traceback.print_exc()


    def getEntGraphG(self, keyword, attIds, level, nodeType):
        try:
            # 建立socket
            transport = TSocket.TSocket('', 9011)
            # 选择传输协议，和服务端一致
            transport = TTransport.TBufferedTransport(transport)
            # protocol = TBinaryProtocol.TBinaryProtocol(transport)
            protocol = TCompactProtocol.TCompactProtocol(transport)
            # 创建客户端
            client = Interface.Client(protocol)
            transport.open()
            data = client.getEntGraphG(keyword, attIds, level, nodeType)
            print(data)
            transport.close()
            return data
        # 捕获异常
        except:
            traceback.print_exc()


    def getEntsRelevanceSeekGraphG(self, entName, attIds, level):
        try:
            # 建立socket
            transport = TSocket.TSocket('', 9011)
            # 选择传输协议，和服务端一致
            transport = TTransport.TBufferedTransport(transport)
            # protocol = TBinaryProtocol.TBinaryProtocol(transport)
            protocol = TCompactProtocol.TCompactProtocol(transport)
            # 创建客户端
            client = Interface.Client(protocol)
            transport.open()
            data = client.getEntsRelevanceSeekGraphG(entName, attIds, level)
            print(data)
            transport.close()
            return data
        # 捕获异常
        except:
            traceback.print_exc()


if __name__ == '__main__':

    cli = PyClient()
    import time
    s = time.time()
    cli.getEntActualContoller("晟睿电气科技（江苏）有限公司", "", 0)
    print(time.time() - s)
    s = time.time()
    # cli.getEntGraphG('镇江市广播电视服务公司经营部', 'R107;R108;R106', '3', 'GS')
    print(time.time() - s)
    s = time.time()
    # cli.getEntsRelevanceSeekGraphG('镇江新区鸿业精密机械厂;镇江润豪建筑劳务有限公司', 'R102;R101;R107;R108;R104;R103;R106;R105', '6')
    e = time.time()
    print(e-s)
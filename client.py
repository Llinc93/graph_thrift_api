import traceback

from com.thrift.interface.server import Interface

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


class PyClient():


    def __init__(self):

        # self.host = '47.93.228.56'
        # self.post = 9918
        self.host = '127.0.0.1'
        self.port = 9918

    def getEntActualContoller(self, username, uscCode, mix_rate):
        try:
            # 建立socket
            transport = TSocket.TSocket(self.host, self.port)
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
            transport = TSocket.TSocket(self.host, self.port)
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
            transport = TSocket.TSocket(self.host, self.port)
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
    ent = []
    cli = PyClient()
    import time
    # for i in ent:
    #     s = time.time()
    #     cli.getEntActualContoller(i, "", 0)
    #     print(time.time() - s)
    #     print()
    # s = time.time()
    cli.getEntGraphG('江苏荣马城市建设有限公司', 'R101;R102;R103;R104;R106;R107', '2', 'GS')
    # s = time.time()
    # cli.getEntsRelevanceSeekGraphG('江苏荣马城市建设有限公司;江苏荣马实业有限公司', 'R102;R101;R107;R108;R104;R103;R106;R105', '6')
    # e = time.time()
    # print(e-s)
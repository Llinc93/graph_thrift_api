import traceback

from com.thrift.interface.server import Interface

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


class PyClient():


    def __init__(self):

        # self.host = '47.93.228.56'
        # self.post = 9918
        self.host = '121.52.214.42'
        self.port = 8140

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

    def getFinalBeneficiaryName(self, username, uscCode, mix_rate):
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
            data = client.getFinalBeneficiaryName(username, uscCode, mix_rate)
            print(data)
            transport.close()
            return data
        # 捕获异常
        except:
            traceback.print_exc()


if __name__ == '__main__':
    ent = ['江苏荣马城市建设有限公司', '江苏鑫茂能源有限公司', '无锡亚琴海办公商贸有限公司', '苏州勇德云服饰有限公司', '苏州博一永畅科技有限公司', '江苏臻天机科技有限公司', '南京晨光集团有限公司', '启东市明珠浴室']
    cli = PyClient()
    import time
    index = 0
    for i in ent:
        index += 1
        if index != 1:
            continue
        i = '宁波市赛伯乐招宝创业投资管理有限公司'
        s = time.time()
        cli.getEntActualContoller(i, "", 0)
        print(time.time() - s)
        print()
        s = time.time()
        cli.getFinalBeneficiaryName(i, "", 0)
        print(time.time() - s)

        s = time.time()
        cli.getEntGraphG(i, 'R101;R102;R103;R104;R106;R107;R108', '2', 'GS')
        print(time.time() - s)
        print()
    relevance = ''
    # s = time.time()
    # cli.getEntsRelevanceSeekGraphG(relevance, 'R102;R101;R107;R108;R104;R103;R106;R105', '6')
    # e = time.time()
    # print(e-s)
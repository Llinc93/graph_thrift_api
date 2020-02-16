import traceback

from com.thrift.interface.server import Interface

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol


class PyClient():


    def __init__(self):

        self.host = '172.27.2.2'
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

    cli = PyClient()
    import time
    for i in ["广州市新德成体育培训有限公司", "上海电影股份有限公司", "华为技术有限公司", "九次方大数据信息集团有限公司", "合肥电信电子通信研究所加工厂", "钦州市钦北区耀清养殖场", "北京缤果互动科技有限公司", "昆明琼云经贸有限公司南屏街服装店", "北京信中利益信股权投资中心（有限合伙）", "烟台明石达远投资中心（有限合伙）", "济南建华创业投资合伙企业（有限合伙）", "中投建华（湖南）创业投资合伙企业（有限合伙）"]:
        s = time.time()
        cli.getEntActualContoller(i, "", 0)
        print(time.time() - s)
        print()
    # s = time.time()
    # cli.getEntGraphG('镇江市广播电视服务公司经营部', 'R107;R108;R106', '3', 'GS')
    # print(time.time() - s)
    # s = time.time()
    # cli.getEntsRelevanceSeekGraphG('镇江新区鸿业精密机械厂;镇江润豪建筑劳务有限公司', 'R102;R101;R107;R108;R104;R103;R106;R105', '6')
    # e = time.time()
    # print(e-s)
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import common.Interface;


public class ThriftClient {

    public void getEntActualContoller(String username, String uscCode, double MinRatio) {
        TTransport transport = null;
        try {
            transport = new TSocket("127.0.0.1", 9011, 3000);

            TProtocol protocol = new TCompactProtocol(transport);
            Interface.Client client = new Interface.Client(protocol);
            transport.open();
            String result = client.getEntActualContoller(username, uscCode, MinRatio);
            System.out.println("getEntActualContoller result=" + result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }

    public void getEntGraphG(String keyword, String attIds, String level, String nodeType) {
        TTransport transport = null;
        try {
            transport = new TSocket("127.0.0.1", 9011, 3000);

            TProtocol protocol = new TCompactProtocol(transport);
            Interface.Client client = new Interface.Client(protocol);
            transport.open();
            String result = client.getEntGraphG(keyword, attIds, level, nodeType);
            System.out.println("getEntGraphG result=" + result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }

    public void getEntsRelevanceSeekGraphG(String entName, String attIds, String level) {
        TTransport transport = null;
        try {
            transport = new TSocket("127.0.0.1", 9011, 3000);

            TProtocol protocol = new TCompactProtocol(transport);
            Interface.Client client = new Interface.Client(protocol);
            transport.open();
            String result = client.getEntsRelevanceSeekGraphG(entName, attIds, level);
            System.out.println("getEntsRelevanceSeekGraphG result=" + result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }


    public static void main(String[] args) {
        System.out.println("thrift client init ");
        ThriftClient client = new ThriftClient();
        System.out.println("thrift client start ");
        //企业实际控制人信息
        client.getEntActualContoller("晟睿电气科技（江苏）有限公司", "", 0);
        //企业图谱查询
        client.getEntGraphG("镇江市广播电视服务公司修理部", "R102;R101;R107;R108;R104;R103;R106;R105", "3", "GS");
        //企业关联探寻
        client.getEntsRelevanceSeekGraphG("镇江市广播电视服务公司修理部;江苏省医药有限公司", "R102;R101;R107;R108;R104;R103;R106;R105", "3");
        System.out.println("thrift client end ");
    }

}
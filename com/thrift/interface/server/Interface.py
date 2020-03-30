#
# Autogenerated by Thrift Compiler (0.13.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    def getEntActualContoller(self, entName, uscCode, MinRatio):
        """
        企业实际控制人信息
        entName 企业名称
        uscCode 社会信用代码
        MinRatio 最小占比 0-100 默认值请传入0

        Parameters:
         - entName
         - uscCode
         - MinRatio

        """
        pass

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
        pass

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
        pass

    def getFinalBeneficiaryName(self, entName, uscCode, MinRatio):
        """
        企业最终受益人信息
        entName 企业名称
        uscCode 社会信用代码
        MinRatio 最小占比 0-100 默认值请传入0

        Parameters:
         - entName
         - uscCode
         - MinRatio

        """
        pass


class Client(Iface):
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def getEntActualContoller(self, entName, uscCode, MinRatio):
        """
        企业实际控制人信息
        entName 企业名称
        uscCode 社会信用代码
        MinRatio 最小占比 0-100 默认值请传入0

        Parameters:
         - entName
         - uscCode
         - MinRatio

        """
        self.send_getEntActualContoller(entName, uscCode, MinRatio)
        return self.recv_getEntActualContoller()

    def send_getEntActualContoller(self, entName, uscCode, MinRatio):
        self._oprot.writeMessageBegin('getEntActualContoller', TMessageType.CALL, self._seqid)
        args = getEntActualContoller_args()
        args.entName = entName
        args.uscCode = uscCode
        args.MinRatio = MinRatio
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getEntActualContoller(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getEntActualContoller_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.e is not None:
            raise result.e
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getEntActualContoller failed: unknown result")

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
        self.send_getEntGraphG(keyword, attIds, level, nodeType)
        return self.recv_getEntGraphG()

    def send_getEntGraphG(self, keyword, attIds, level, nodeType):
        self._oprot.writeMessageBegin('getEntGraphG', TMessageType.CALL, self._seqid)
        args = getEntGraphG_args()
        args.keyword = keyword
        args.attIds = attIds
        args.level = level
        args.nodeType = nodeType
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getEntGraphG(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getEntGraphG_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.e is not None:
            raise result.e
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getEntGraphG failed: unknown result")

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
        self.send_getEntsRelevanceSeekGraphG(entName, attIds, level)
        return self.recv_getEntsRelevanceSeekGraphG()

    def send_getEntsRelevanceSeekGraphG(self, entName, attIds, level):
        self._oprot.writeMessageBegin('getEntsRelevanceSeekGraphG', TMessageType.CALL, self._seqid)
        args = getEntsRelevanceSeekGraphG_args()
        args.entName = entName
        args.attIds = attIds
        args.level = level
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getEntsRelevanceSeekGraphG(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getEntsRelevanceSeekGraphG_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.e is not None:
            raise result.e
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getEntsRelevanceSeekGraphG failed: unknown result")

    def getFinalBeneficiaryName(self, entName, uscCode, MinRatio):
        """
        企业最终受益人信息
        entName 企业名称
        uscCode 社会信用代码
        MinRatio 最小占比 0-100 默认值请传入0

        Parameters:
         - entName
         - uscCode
         - MinRatio

        """
        self.send_getFinalBeneficiaryName(entName, uscCode, MinRatio)
        return self.recv_getFinalBeneficiaryName()

    def send_getFinalBeneficiaryName(self, entName, uscCode, MinRatio):
        self._oprot.writeMessageBegin('getFinalBeneficiaryName', TMessageType.CALL, self._seqid)
        args = getFinalBeneficiaryName_args()
        args.entName = entName
        args.uscCode = uscCode
        args.MinRatio = MinRatio
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def recv_getFinalBeneficiaryName(self):
        iprot = self._iprot
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(iprot)
            iprot.readMessageEnd()
            raise x
        result = getFinalBeneficiaryName_result()
        result.read(iprot)
        iprot.readMessageEnd()
        if result.success is not None:
            return result.success
        if result.e is not None:
            raise result.e
        raise TApplicationException(TApplicationException.MISSING_RESULT, "getFinalBeneficiaryName failed: unknown result")


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["getEntActualContoller"] = Processor.process_getEntActualContoller
        self._processMap["getEntGraphG"] = Processor.process_getEntGraphG
        self._processMap["getEntsRelevanceSeekGraphG"] = Processor.process_getEntsRelevanceSeekGraphG
        self._processMap["getFinalBeneficiaryName"] = Processor.process_getFinalBeneficiaryName
        self._on_message_begin = None

    def on_message_begin(self, func):
        self._on_message_begin = func

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if self._on_message_begin:
            self._on_message_begin(name, type, seqid)
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_getEntActualContoller(self, seqid, iprot, oprot):
        args = getEntActualContoller_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getEntActualContoller_result()
        try:
            result.success = self._handler.getEntActualContoller(args.entName, args.uscCode, args.MinRatio)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except AuditException as e:
            msg_type = TMessageType.REPLY
            result.e = e
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getEntActualContoller", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_getEntGraphG(self, seqid, iprot, oprot):
        args = getEntGraphG_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getEntGraphG_result()
        try:
            result.success = self._handler.getEntGraphG(args.keyword, args.attIds, args.level, args.nodeType)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except AuditException as e:
            msg_type = TMessageType.REPLY
            result.e = e
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getEntGraphG", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_getEntsRelevanceSeekGraphG(self, seqid, iprot, oprot):
        args = getEntsRelevanceSeekGraphG_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getEntsRelevanceSeekGraphG_result()
        try:
            result.success = self._handler.getEntsRelevanceSeekGraphG(args.entName, args.attIds, args.level)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except AuditException as e:
            msg_type = TMessageType.REPLY
            result.e = e
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getEntsRelevanceSeekGraphG", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

    def process_getFinalBeneficiaryName(self, seqid, iprot, oprot):
        args = getFinalBeneficiaryName_args()
        args.read(iprot)
        iprot.readMessageEnd()
        result = getFinalBeneficiaryName_result()
        try:
            result.success = self._handler.getFinalBeneficiaryName(args.entName, args.uscCode, args.MinRatio)
            msg_type = TMessageType.REPLY
        except TTransport.TTransportException:
            raise
        except AuditException as e:
            msg_type = TMessageType.REPLY
            result.e = e
        except TApplicationException as ex:
            logging.exception('TApplication exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = ex
        except Exception:
            logging.exception('Unexpected exception in handler')
            msg_type = TMessageType.EXCEPTION
            result = TApplicationException(TApplicationException.INTERNAL_ERROR, 'Internal error')
        oprot.writeMessageBegin("getFinalBeneficiaryName", msg_type, seqid)
        result.write(oprot)
        oprot.writeMessageEnd()
        oprot.trans.flush()

# HELPER FUNCTIONS AND STRUCTURES


class getEntActualContoller_args(object):
    """
    Attributes:
     - entName
     - uscCode
     - MinRatio

    """


    def __init__(self, entName=None, uscCode=None, MinRatio=None,):
        self.entName = entName
        self.uscCode = uscCode
        self.MinRatio = MinRatio

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.entName = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.uscCode = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.DOUBLE:
                    self.MinRatio = iprot.readDouble()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntActualContoller_args')
        if self.entName is not None:
            oprot.writeFieldBegin('entName', TType.STRING, 1)
            oprot.writeString(self.entName.encode('utf-8') if sys.version_info[0] == 2 else self.entName)
            oprot.writeFieldEnd()
        if self.uscCode is not None:
            oprot.writeFieldBegin('uscCode', TType.STRING, 2)
            oprot.writeString(self.uscCode.encode('utf-8') if sys.version_info[0] == 2 else self.uscCode)
            oprot.writeFieldEnd()
        if self.MinRatio is not None:
            oprot.writeFieldBegin('MinRatio', TType.DOUBLE, 3)
            oprot.writeDouble(self.MinRatio)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.entName is None:
            raise TProtocolException(message='Required field entName is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntActualContoller_args)
getEntActualContoller_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'entName', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'uscCode', 'UTF8', None, ),  # 2
    (3, TType.DOUBLE, 'MinRatio', None, None, ),  # 3
)


class getEntActualContoller_result(object):
    """
    Attributes:
     - success
     - e

    """


    def __init__(self, success=None, e=None,):
        self.success = success
        self.e = e

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.e = AuditException()
                    self.e.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntActualContoller_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeString(self.success.encode('utf-8') if sys.version_info[0] == 2 else self.success)
            oprot.writeFieldEnd()
        if self.e is not None:
            oprot.writeFieldBegin('e', TType.STRUCT, 1)
            self.e.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntActualContoller_result)
getEntActualContoller_result.thrift_spec = (
    (0, TType.STRING, 'success', 'UTF8', None, ),  # 0
    (1, TType.STRUCT, 'e', [AuditException, None], None, ),  # 1
)


class getEntGraphG_args(object):
    """
    Attributes:
     - keyword
     - attIds
     - level
     - nodeType

    """


    def __init__(self, keyword=None, attIds=None, level=None, nodeType=None,):
        self.keyword = keyword
        self.attIds = attIds
        self.level = level
        self.nodeType = nodeType

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.keyword = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.attIds = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.level = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRING:
                    self.nodeType = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntGraphG_args')
        if self.keyword is not None:
            oprot.writeFieldBegin('keyword', TType.STRING, 1)
            oprot.writeString(self.keyword.encode('utf-8') if sys.version_info[0] == 2 else self.keyword)
            oprot.writeFieldEnd()
        if self.attIds is not None:
            oprot.writeFieldBegin('attIds', TType.STRING, 2)
            oprot.writeString(self.attIds.encode('utf-8') if sys.version_info[0] == 2 else self.attIds)
            oprot.writeFieldEnd()
        if self.level is not None:
            oprot.writeFieldBegin('level', TType.STRING, 3)
            oprot.writeString(self.level.encode('utf-8') if sys.version_info[0] == 2 else self.level)
            oprot.writeFieldEnd()
        if self.nodeType is not None:
            oprot.writeFieldBegin('nodeType', TType.STRING, 4)
            oprot.writeString(self.nodeType.encode('utf-8') if sys.version_info[0] == 2 else self.nodeType)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntGraphG_args)
getEntGraphG_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'keyword', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'attIds', 'UTF8', None, ),  # 2
    (3, TType.STRING, 'level', 'UTF8', None, ),  # 3
    (4, TType.STRING, 'nodeType', 'UTF8', None, ),  # 4
)


class getEntGraphG_result(object):
    """
    Attributes:
     - success
     - e

    """


    def __init__(self, success=None, e=None,):
        self.success = success
        self.e = e

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.e = AuditException()
                    self.e.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntGraphG_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeString(self.success.encode('utf-8') if sys.version_info[0] == 2 else self.success)
            oprot.writeFieldEnd()
        if self.e is not None:
            oprot.writeFieldBegin('e', TType.STRUCT, 1)
            self.e.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntGraphG_result)
getEntGraphG_result.thrift_spec = (
    (0, TType.STRING, 'success', 'UTF8', None, ),  # 0
    (1, TType.STRUCT, 'e', [AuditException, None], None, ),  # 1
)


class getEntsRelevanceSeekGraphG_args(object):
    """
    Attributes:
     - entName
     - attIds
     - level

    """


    def __init__(self, entName=None, attIds=None, level=None,):
        self.entName = entName
        self.attIds = attIds
        self.level = level

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.entName = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.attIds = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.level = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntsRelevanceSeekGraphG_args')
        if self.entName is not None:
            oprot.writeFieldBegin('entName', TType.STRING, 1)
            oprot.writeString(self.entName.encode('utf-8') if sys.version_info[0] == 2 else self.entName)
            oprot.writeFieldEnd()
        if self.attIds is not None:
            oprot.writeFieldBegin('attIds', TType.STRING, 2)
            oprot.writeString(self.attIds.encode('utf-8') if sys.version_info[0] == 2 else self.attIds)
            oprot.writeFieldEnd()
        if self.level is not None:
            oprot.writeFieldBegin('level', TType.STRING, 3)
            oprot.writeString(self.level.encode('utf-8') if sys.version_info[0] == 2 else self.level)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntsRelevanceSeekGraphG_args)
getEntsRelevanceSeekGraphG_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'entName', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'attIds', 'UTF8', None, ),  # 2
    (3, TType.STRING, 'level', 'UTF8', None, ),  # 3
)


class getEntsRelevanceSeekGraphG_result(object):
    """
    Attributes:
     - success
     - e

    """


    def __init__(self, success=None, e=None,):
        self.success = success
        self.e = e

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.e = AuditException()
                    self.e.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getEntsRelevanceSeekGraphG_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeString(self.success.encode('utf-8') if sys.version_info[0] == 2 else self.success)
            oprot.writeFieldEnd()
        if self.e is not None:
            oprot.writeFieldBegin('e', TType.STRUCT, 1)
            self.e.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getEntsRelevanceSeekGraphG_result)
getEntsRelevanceSeekGraphG_result.thrift_spec = (
    (0, TType.STRING, 'success', 'UTF8', None, ),  # 0
    (1, TType.STRUCT, 'e', [AuditException, None], None, ),  # 1
)


class getFinalBeneficiaryName_args(object):
    """
    Attributes:
     - entName
     - uscCode
     - MinRatio

    """


    def __init__(self, entName=None, uscCode=None, MinRatio=None,):
        self.entName = entName
        self.uscCode = uscCode
        self.MinRatio = MinRatio

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.entName = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.uscCode = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.DOUBLE:
                    self.MinRatio = iprot.readDouble()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getFinalBeneficiaryName_args')
        if self.entName is not None:
            oprot.writeFieldBegin('entName', TType.STRING, 1)
            oprot.writeString(self.entName.encode('utf-8') if sys.version_info[0] == 2 else self.entName)
            oprot.writeFieldEnd()
        if self.uscCode is not None:
            oprot.writeFieldBegin('uscCode', TType.STRING, 2)
            oprot.writeString(self.uscCode.encode('utf-8') if sys.version_info[0] == 2 else self.uscCode)
            oprot.writeFieldEnd()
        if self.MinRatio is not None:
            oprot.writeFieldBegin('MinRatio', TType.DOUBLE, 3)
            oprot.writeDouble(self.MinRatio)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.entName is None:
            raise TProtocolException(message='Required field entName is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getFinalBeneficiaryName_args)
getFinalBeneficiaryName_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'entName', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'uscCode', 'UTF8', None, ),  # 2
    (3, TType.DOUBLE, 'MinRatio', None, None, ),  # 3
)


class getFinalBeneficiaryName_result(object):
    """
    Attributes:
     - success
     - e

    """


    def __init__(self, success=None, e=None,):
        self.success = success
        self.e = e

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 0:
                if ftype == TType.STRING:
                    self.success = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 1:
                if ftype == TType.STRUCT:
                    self.e = AuditException()
                    self.e.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('getFinalBeneficiaryName_result')
        if self.success is not None:
            oprot.writeFieldBegin('success', TType.STRING, 0)
            oprot.writeString(self.success.encode('utf-8') if sys.version_info[0] == 2 else self.success)
            oprot.writeFieldEnd()
        if self.e is not None:
            oprot.writeFieldBegin('e', TType.STRUCT, 1)
            self.e.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(getFinalBeneficiaryName_result)
getFinalBeneficiaryName_result.thrift_spec = (
    (0, TType.STRING, 'success', 'UTF8', None, ),  # 0
    (1, TType.STRUCT, 'e', [AuditException, None], None, ),  # 1
)
fix_spec(all_structs)
del all_structs


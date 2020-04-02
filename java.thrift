namespace java common

exception AuditException {
    1: required string why
}


struct EntGraphGParams {
    1:string keyword,
    2:string attIds,
    3:string level,
     4:string nodeType
}

struct EntsRelevanceSeekGraphGParams {
    1:string entName,
    2:string attIds,
    3:string level
}

struct getFinalBeneficiaryName {
    1: string entName,
    2: string uscCode,
    3: double MinRatio
}

service Interface {

    /**
    * 图数据库查询接口
    */

    /**
    * 企业实际控制人信息
    * entName 企业名称
    * uscCode 社会信用代码
    * MinRatio 最小占比 0-100 默认值请传入0
    */
    string getEntActualContoller(1: required string entName, 2:string uscCode, 3:double MinRatio) throws (1:AuditException e)
    /**
    * 企业图谱查询
    * keyword 关键字
    * attIds 过滤关系
    * level 层级，最大3层
    * nodeType 节点类型
    */
    string getEntGraphG(1:string keyword, 2:string attIds, 3:string level, 4:string nodeType) throws (1:AuditException e)

    /**
    * 企业关联探寻
    * entName 企业名称
    * attIds 过滤关系
    * level 层级，最大6层
    */
    string getEntsRelevanceSeekGraphG(1:string entName, 2:string attIds, 3:string level) throws (1:AuditException e)

    /**
    * 企业最终受益人信息
    * entName 企业名称
    * uscCode 社会信用代码
    * MinRatio 最小占比 0-100 默认值请传入0
    */
    string getFinalBeneficiaryName(1: required string entName, 2:string uscCode, 3:double MinRatio) throws (1:AuditException e)

}


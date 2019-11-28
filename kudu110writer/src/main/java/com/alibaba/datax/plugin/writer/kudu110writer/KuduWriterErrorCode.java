package com.alibaba.datax.plugin.writer.kudu110writer;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KuduWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("KuduWriter-00", "您的参数配置错误."),
    KUDU_ERROR_TABLE("KuduWriter-01", "您缺失了必须填写的参数值-kudu表名称."),
    ILLEGAL_VALUE("KuduWriter-02", "您填写的参数值不合法."),
    KUDU_ERROR_MASTER("KuduWriter-03", "您缺失了必须填写的参数值-kudu的master服务地址."),
    KUDU_ERROR_PRIMARY_KEY("KuduWriter-04", "您缺失了必须填写的参数值-kudu表的主键."),
    KUDU_ERROR_CONF_COLUMNS("KuduWriter-05", "您缺失了必须填写的参数值-列要包含index,name,type."),
    ILLEGAL_VALUES_ERROR("KuduWriter-06", "您填写的参数个数或值不合法."),
    WRITE_DATA_ERROR("DBUtilErrorCode-07", "往您配置的写入表中写入数据时失败."),
    EXTERNAL_CONSISTENCY_MODE("KuduWriter-08", "Kudu Session设置一致性模式时失败.");

    private final String code;
    private final String description;

    private KuduWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}

package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.core.api.Registerable;
import org.apache.spark.sql.types.DataType;

/**
 * @author lizheng
 * @date 2019/7/11
 */
public abstract class AbstractUdf implements Registerable {

    /**
     * 类型
     *
     * @return
     */
    abstract public DataType getDataType();

    /**
     * 使用方法
     *
     * @return
     */
    abstract protected String use_desc();

    /**
     * Help描述
     *
     * @return
     */
    final public String desc() {

        StringBuilder descSb = new StringBuilder();

        descSb.append("方法名: ")
                .append(getRegisterName())
                .append(",  ")
                .append("使用说明 :")
                .append(use_desc());

        return descSb.toString();
    }
}

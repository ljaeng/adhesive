package com.jaeng.adhesive.core;

import org.apache.spark.sql.types.DataType;

/**
 * 支持注册
 *
 * @author lizheng
 * @date 2019/6/9
 */
public interface Registerable {

    /**
     * 注册名称
     *
     * @return
     */
    String getRegisterName();

    /**
     * 类型
     *
     * @return
     */
    DataType getDataType();
}

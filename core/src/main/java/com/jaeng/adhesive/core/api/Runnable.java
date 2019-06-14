package com.jaeng.adhesive.core.api;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * 任务
 *
 * @author lizheng
 * @date 2019/6/6
 */
public interface Runnable {

    /**
     * 执行
     *
     * @param sparkSession
     * @param context
     */
    void run(SparkSession sparkSession, Map<String, Object> context);

}

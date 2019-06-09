package com.jaeng.adhesive.core;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public interface Runnable {

    void run(SparkSession sparkSession, Map<String, Object> context);

}

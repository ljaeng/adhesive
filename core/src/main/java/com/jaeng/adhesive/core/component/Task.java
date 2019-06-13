package com.jaeng.adhesive.core.component;

import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public interface Task {

    void process(AbstractJob job, String sql, SparkSession sparkSession, Map<String, Object> context);
}

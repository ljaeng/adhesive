package com.jaeng.adhesive.core.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class JsonSource extends AbstractSource {
    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = sparkSession.read().json(path.split(","));

        super.registerTable(context, dataset);
    }
}

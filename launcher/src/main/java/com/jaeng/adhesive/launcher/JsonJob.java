package com.jaeng.adhesive.launcher;

import com.jaeng.adhesive.core.component.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JsonJob extends AbstractJob {

    @Override
    public void init(Object config) {
        super.init(config);
        //TODO
        super.initSparkSession();
    }


    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

    }
}

package com.jaeng.adhesive.core.job;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class FileJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(FileJob.class);

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
    }


}

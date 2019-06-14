package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class PluginTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);
    }
}

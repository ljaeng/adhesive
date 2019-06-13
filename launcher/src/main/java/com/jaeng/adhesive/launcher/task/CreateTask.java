package com.jaeng.adhesive.launcher.task;

import com.jaeng.adhesive.core.component.AbstractJob;
import com.jaeng.adhesive.core.component.AbstractTask;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class CreateTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String sql, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, sql, sparkSession, context);
    }
}

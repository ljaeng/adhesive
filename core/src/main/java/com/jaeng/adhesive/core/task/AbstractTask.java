package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.api.Task;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class AbstractTask implements Task {

    private AbstractJob job;

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        this.job = job;
    }

    public Component getComponent(String type, String name) throws Exception {
        return this.job.getComponent(type, name);
    }
}

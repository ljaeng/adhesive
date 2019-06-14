package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class GetTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        String[] split = line.split(" +");
        if (split.length > 1) {
            String key = split[1];
            Object value = context.containsKey(key) ? context.get(key) : "NULL";
            System.out.println(value);
        }
    }
}

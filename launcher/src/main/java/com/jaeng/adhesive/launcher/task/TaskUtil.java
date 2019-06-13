package com.jaeng.adhesive.launcher.task;

import com.jaeng.adhesive.common.util.BeetlTemplateUtil;
import com.jaeng.adhesive.core.component.AbstractJob;
import com.jaeng.adhesive.core.component.Task;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class TaskUtil {

    public static void process(AbstractJob job, String sql, SparkSession sparkSession, Map<String, Object> context) {

        sql = sql.trim().replaceAll(";+$", "").toLowerCase();
        sql = BeetlTemplateUtil.render(context, sql);

        Task task;
        if (sql.startsWith("insert")) {
            task = new InsertTask();
        } else if (sql.startsWith("set")) {
            task = new SetTask();
        } else if (sql.startsWith("get")) {
            task = new GetTask();
        } else if (sql.startsWith("create")) {
            task = new CreateTask();
        } else if (sql.startsWith("plugin")) {
            task = new PluginTask();
        } else if (sql.startsWith("process")) {
            task = new ProcessTask();
        } else if (sql.startsWith("hdfs")) {
            task = new HdfsTask();
        } else if (sql.startsWith("clear")) {
            task = new ClearTask();
        } else if (sql.startsWith("help")) {
            task = new HelpTask();
        } else {
            task = new ProcessTask();
        }
        task.process(job, sql, sparkSession, context);
    }

}

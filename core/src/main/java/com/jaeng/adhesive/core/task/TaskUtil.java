package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.common.util.BeetlTemplateUtil;
import com.jaeng.adhesive.core.api.Task;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class TaskUtil {

    public static void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {

        line = line.trim().replaceAll(";+$", "");
        line = BeetlTemplateUtil.render(context, line);

        String action = line.split(" ")[0];
        String actionLow = action.toLowerCase();
        line = line.replaceFirst(action, actionLow);

        Task task;
        if (line.startsWith("save")) {
            //TODO:done
            task = new SaveTask();
        } else if (line.startsWith("set")) {
            //TODO:done
            task = new SetTask();
        } else if (line.startsWith("get")) {
            //TODO:done
            task = new GetTask();
        } else if (line.startsWith("create")) {
            //TODO:done
            task = new CreateTask();
        } else if (line.startsWith("plugin")) {
            task = new PluginTask();
        } else if (line.startsWith("process")) {
            //TODO:
            task = new ProcessTask();
        } else if (line.startsWith("hdfs")) {
            task = new HdfsTask();
        } else if (line.startsWith("clear")) {
            //TODO:done
            task = new ClearTask();
        } else if (line.startsWith("show")) {
            //TODO:done
            task = new ShowTask();
        } else if (line.startsWith("help")) {
            //TODO:done
            task = new HelpTask();
        } else {
            //TODO:done
            task = new ConsoleTask();
        }
        task.process(job, line, sparkSession, context);
    }

}

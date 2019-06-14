package com.jaeng.adhesive.core.task;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.enums.ComponentTypeEnum;
import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class ConsoleTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        Component component = null;
        try {
            component = super.getComponent(ComponentTypeEnum.PROCESS.getType(), "console");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (component != null) {
            JSONObject conf = new JSONObject();
            conf.put("sql", line);
            component.setConf(conf);
            component.run(sparkSession, context);
        }
    }
}

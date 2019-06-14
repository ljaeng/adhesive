package com.jaeng.adhesive.core.task;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.enums.ComponentTypeEnum;
import com.jaeng.adhesive.common.util.PatternUtil;
import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class CreateTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);
        JSONObject conf = new JSONObject();
        String componentType;
        String componentName;

        String regex = "create +table +(.*?) *as *(.*)";
        String table = PatternUtil.getValueByRegex(regex, line, 1);
        String sql = PatternUtil.getValueByRegex(regex, line, 2);

        if (sql.contains("`")) {
            componentType = ComponentTypeEnum.SOURCE.getType();

            int index = sql.indexOf("`");
            componentName = sql.substring(0, index - 1);
            String path = sql.substring(index + 1, sql.length() - 1);

            try {
                JSONObject jsonObject = JSONObject.parseObject(path);
                conf.putAll(jsonObject);
            } catch (Exception e) {
                conf.put("path", path);
            }
            conf.put("table", table);
        } else {
            componentType = ComponentTypeEnum.PROCESS.getType();
            componentName = "sql";
            conf.put("table", table);
            conf.put("sql", sql);
        }
        try {
            Component component = job.getComponent(componentType, componentName);
            component.setConf(conf);
            component.run(sparkSession, context);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

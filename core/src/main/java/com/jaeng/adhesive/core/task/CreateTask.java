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
        String componentType = null;
        String componentName = null;

        String tableRegex = "create +table +(.*?) *as *(.*)";
        String broadcastRegex = "create +broadcast +(.*?) *as +(.*?) *. *`(.*)` *(.*)";
        //创建临时表
        if (PatternUtil.findValueByRegex(tableRegex, line)) {

            String table = PatternUtil.getValueByRegex(tableRegex, line, 1);
            String sql = PatternUtil.getValueByRegex(tableRegex, line, 2);

            if (sql.contains("`")) {
                tableRegex = "create +table +(.*?) *as +(.*?) *. *`(.*)` *(.*)";
                componentType = ComponentTypeEnum.SOURCE.getType();
                componentName = PatternUtil.getValueByRegex(tableRegex, line, 2);

                String path = PatternUtil.getValueByRegex(tableRegex, line, 3);
                sql = PatternUtil.getValueByRegex(tableRegex, line, 4);

                try {
                    JSONObject jsonObject = JSONObject.parseObject(path);
                    conf.putAll(jsonObject);
                    conf.put("sql", sql);
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
        } else if (PatternUtil.findValueByRegex(broadcastRegex, line)) {
            //创建广播变量
            componentName = "broadcast";
            componentType = ComponentTypeEnum.PROCESS.getType();

            String broadcastName = PatternUtil.getValueByRegex(broadcastRegex, line, 0);
            String type = PatternUtil.getValueByRegex(broadcastRegex, line, 1);
            String confStr = PatternUtil.getValueByRegex(broadcastRegex, line, 2);
            String sql = PatternUtil.getValueByRegex(broadcastRegex, line, 3);

            try {
                conf.putAll(JSONObject.parseObject(confStr));
            } catch (Exception e) {
            }

            conf.put("sql", sql);
            conf.put("broadcastName", broadcastName);
            conf.put("type", type);
        }
        if (componentType != null && componentName != null) {
            try {
                Component component = job.getComponent(componentType, componentName);
                component.setConf(conf);
                component.run(sparkSession, context);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

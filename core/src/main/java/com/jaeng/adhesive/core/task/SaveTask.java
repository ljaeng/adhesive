package com.jaeng.adhesive.core.task;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.enums.ComponentTypeEnum;
import com.jaeng.adhesive.common.util.PatternUtil;
import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class SaveTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        String regex = "save +(.*?) *. *`(.*)` *(.*)";
        String name = PatternUtil.getValueByRegex(regex, line, 1);
        String confStr = PatternUtil.getValueByRegex(regex, line, 2);
        String sql = PatternUtil.getValueByRegex(regex, line, 3);

        try {
            Component component = job.getComponent(ComponentTypeEnum.SINK.getType(), name);
            JSONObject conf = null;
            try {
                conf = JSONObject.parseObject(confStr);
            } catch (Exception e) {
                conf = new JSONObject();
                conf.put("path", confStr);
            }

            if (StringUtils.isNotBlank(sql)) {
                conf.put("sql", sql);
            }
            component.setConf(conf);
            component.run(sparkSession, context);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

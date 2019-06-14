package com.jaeng.adhesive.core.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.api.Component;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JsonJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(JsonJob.class);

    private JSONArray jobConfig;

    @Override
    public void init(Object config) {
        super.init(config);
        try {
            String jobConfigStr = FileUtils.readFileToString(new File(super.conf.getConfig()));
            jobConfig = JSONArray.parseArray(jobConfigStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        if (jobConfig != null) {
            for (int i = 0; i < jobConfig.size(); i++) {
                JSONObject job = jobConfig.getJSONObject(i);
                try {
                    Component component = this.getComponent(job.getString("type"), job.getString("name"));
                    component.setConf(job);
                    component.run(sparkSession, context);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

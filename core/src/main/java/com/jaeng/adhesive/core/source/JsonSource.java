package com.jaeng.adhesive.core.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class JsonSource extends AbstractSource {

    private boolean split;
    private String splitStr;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        split = MapUtils.getBooleanValue(conf, "split", false);
        splitStr = MapUtils.getString(conf, "splitStr", ",");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset;
        if (split) {
            dataset = sparkSession.read().json(path.split(splitStr));
        } else {
            dataset = sparkSession.read().json(path);
        }

        super.registerTable(context, dataset);
    }
}

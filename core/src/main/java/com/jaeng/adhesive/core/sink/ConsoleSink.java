package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class ConsoleSink extends AbstractSink {

    private int line;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        line = MapUtils.getIntValue(conf, "line", 30);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null) {
            dataset.show(line);
        }
    }
}

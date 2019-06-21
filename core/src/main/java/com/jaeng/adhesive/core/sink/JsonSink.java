package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class JsonSink extends AbstractSink {

    private boolean append;
    private boolean overwrite;
    private String path;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        append = MapUtils.getBooleanValue(conf, "append", false);
        this.overwrite = MapUtils.getBooleanValue(conf, "overwrite", false);
        path = MapUtils.getString(conf, "path", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null) {
            if (append) {
                dataset.write().mode(SaveMode.Append).json(path);
            } else if (overwrite) {
                dataset.write().mode(SaveMode.Overwrite).json(path);
            } else {
                dataset.write().json(path);
            }
        }
    }
}

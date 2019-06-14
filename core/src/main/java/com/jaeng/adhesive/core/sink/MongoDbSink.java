package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class MongoDbSink extends AbstractSink {

    private String url;
    private String collection;
    private String database;
    private boolean isReplace;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.url = MapUtils.getString(conf, "url", "");
        this.database = MapUtils.getString(conf, "database", "");
        this.collection = MapUtils.getString(conf, "collection", "");
        this.isReplace = MapUtils.getBooleanValue(conf, "isReplace", false);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);

        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("database", database);
        writeOverrides.put("collection", collection);
        if (dataset != null) {
            WriteConfig writeConf = WriteConfig.create(sparkSession.sqlContext()).withOptions(writeOverrides);
            if (!isReplace) {
                writeConf = writeConf.withOption("replaceDocument", "false");
            }
            MongoSpark.save(dataset, writeConf);
        }
    }
}

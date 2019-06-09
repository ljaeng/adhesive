package com.jaeng.adhesive.input;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.component.AbstractInput;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class MongoInput extends AbstractInput {

    private String url;
    private String database;
    private String collection;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.url = MapUtils.getString(this.conf, "url", "");
        this.database = MapUtils.getString(this.conf, "database", "");
        this.collection = MapUtils.getString(this.conf, "collection", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Map<String, String> readConfigMap = new HashMap<>();
        readConfigMap.put("uri", url);
        readConfigMap.put("database", database);
        readConfigMap.put("collection", collection);
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readConfigMap);

        Dataset dataset = MongoSpark.load(jsc, readConfig).toDF();

        super.registerTable(context, dataset);
    }

}

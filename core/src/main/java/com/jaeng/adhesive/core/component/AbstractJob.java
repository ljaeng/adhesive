package com.jaeng.adhesive.core.component;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public abstract class AbstractJob implements Job {
    protected JSONObject conf;
    protected SparkSession sparkSession;


    protected void initSparkSession() {
        SparkSession.Builder builder = SparkSession.builder();
        this.sparkSession = builder.getOrCreate();

        UDFRegistration udfRegistration = sparkSession.sqlContext().udf();
        //TODO:注册Udf
        //udfRegistration.registerJava("get_object_from_json", GetValueFromJsonUDF.class.getName(), DataTypes.StringType);
    }

    @Override
    public void init(Object config) {
    }

    @Override
    public void run() {
        Map<String, Object> context = new HashMap<>(50);
        run(this.sparkSession, context);
    }

    @Override
    public void release() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}

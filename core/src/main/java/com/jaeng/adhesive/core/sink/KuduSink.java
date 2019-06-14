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
public class KuduSink extends AbstractSink {

    private String master;
    private String kuduTable;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.master = MapUtils.getString(conf, "master", "");
        this.kuduTable = MapUtils.getString(conf, "kuduTable", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null) {
            dataset.write()
                    .format("org.apache.kudu.spark.kudu")
                    .option("kudu.master", master)
                    .option("kudu.table", kuduTable)
                    .mode("append")
                    .save();
        }
    }
}

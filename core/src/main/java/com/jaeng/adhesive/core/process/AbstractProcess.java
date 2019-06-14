package com.jaeng.adhesive.core.process;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.api.Process;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public abstract class AbstractProcess implements Process {

    protected JSONObject conf;

    protected String table;
    protected String dataSetName;

    @Override
    public void setConf(JSONObject conf) {
        this.conf = conf;
        this.table = MapUtils.getString(conf, "table", "");
        this.dataSetName = MapUtils.getString(conf, "dataSetName", table);
    }

    @Override
    public JSONObject getConf() {
        return conf;
    }

    public void registerTable(Map<String, Object> context, Dataset dataset) {
        context.put(LAST_DATASET, dataset);
        context.put(dataSetName, dataset);
        if (dataset != null && StringUtils.isNotEmpty(table)) {
            dataset.createOrReplaceTempView(table);
        }
    }

    public Dataset getProcessDataSet(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset;
        if (StringUtils.isNotEmpty(dataSetName)) {
            dataset = (Dataset) context.get(dataSetName);
        } else {
            dataset = (Dataset) context.get(LAST_DATASET);
        }
        return dataset;

    }
}

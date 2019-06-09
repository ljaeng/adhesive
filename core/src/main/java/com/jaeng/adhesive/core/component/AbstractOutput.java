package com.jaeng.adhesive.core.component;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public abstract class AbstractOutput implements Component {
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

    public Dataset getProcessDataSet(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = null;
        if (StringUtils.isNotEmpty(dataSetName)) {
            dataset = (Dataset) context.get(dataSetName);
        }
        return dataset;

    }
}

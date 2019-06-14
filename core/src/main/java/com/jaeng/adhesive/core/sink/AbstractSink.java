package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.api.Component;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public abstract class AbstractSink implements Component {
    protected JSONObject conf;

    protected String table;
    protected String dataSetName;
    protected String sql;

    @Override
    public void setConf(JSONObject conf) {
        this.conf = conf;
        this.table = MapUtils.getString(conf, "table", "");
        this.dataSetName = MapUtils.getString(conf, "dataSetName", table);
        this.sql = MapUtils.getString(conf, "sql", table);
    }

    @Override
    public JSONObject getConf() {
        return conf;
    }

    public Dataset getProcessDataSet(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset;
        if (StringUtils.isNotBlank(sql)) {
            dataset = sparkSession.sql(sql);
        } else if (StringUtils.isNotBlank(dataSetName)) {
            dataset = (Dataset) context.get(dataSetName);
        } else {
            dataset = (Dataset) context.get(LAST_DATASET);
        }
        return dataset;

    }
}

package com.jaeng.adhesive.core.component;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public abstract class AbstractInput implements Input {

    protected JSONObject conf;

    protected String path;
    protected String table;
    protected String dataSetName;

    @Override
    public void setConf(JSONObject conf) {
        this.conf = conf;

        this.path = MapUtils.getString(conf, "path", "");
        this.table = MapUtils.getString(conf, "table", "");
        this.dataSetName = MapUtils.getString(conf, "dataSetName", table);
    }

    @Override
    public JSONObject getConf() {
        return conf;
    }

    /**
     * 注册临时表
     *
     * @param context
     * @param dataset
     */
    public void registerTable(Map<String, Object> context, Dataset dataset) {
        context.put(dataSetName, dataset);
        if (dataset != null && StringUtils.isNotEmpty(table)) {
            dataset.createOrReplaceTempView(table);
        }
    }
}

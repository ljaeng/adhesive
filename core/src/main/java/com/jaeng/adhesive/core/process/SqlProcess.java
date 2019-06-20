package com.jaeng.adhesive.core.process;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.Constant;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedList;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class SqlProcess extends AbstractProcess {

    private String sql;
    private boolean cache;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.sql = MapUtils.getString(conf, "sql", "");
        this.cache = MapUtils.getBooleanValue(conf, "cache", false);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = null;
        if (StringUtils.isNotBlank(sql)) {
            dataset = sparkSession.sql(sql);
        }
        if (dataset != null) {
            boolean contextCache = context.containsKey("cache") ? Boolean.parseBoolean(context.get("cache").toString()) : false;
            if (cache || contextCache) {
                dataset = dataset.cache();
                LinkedList<Dataset> cacheDataset = (LinkedList<Dataset>) context.get(Constant.CACHE_DATASET_LIST);
                cacheDataset.add(dataset);
                context.remove("cache");
            }
        }
        super.registerTable(context, dataset);
    }
}

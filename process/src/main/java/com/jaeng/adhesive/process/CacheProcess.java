package com.jaeng.adhesive.process;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.component.AbstractProcess;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class CacheProcess extends AbstractProcess {

    private boolean cache;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.cache = MapUtils.getBooleanValue(conf, "cache", false);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        if (cache) {
            Dataset dataset = super.getProcessDataSet(sparkSession, context);
            if (dataset != null) {
                dataset = dataset.cache();
                context.put(dataSetName, dataset);
            }
        }
    }
}

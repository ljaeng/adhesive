package com.jaeng.adhesive.core.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class ConsoleProcess extends AbstractProcess {

    private String sql;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.sql = MapUtils.getString(conf, "sql", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = null;
        if (StringUtils.isNotBlank(sql)) {
            dataset = sparkSession.sql(sql);
        }
        if (dataset != null) {
            dataset.show();
        }
    }
}

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
    private boolean showSchema;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.sql = MapUtils.getString(conf, "sql", "");
        this.showSchema = MapUtils.getBooleanValue(conf, "showSchema", false);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = null;
        if (showSchema) {
            dataset = super.getProcessDataSet(sparkSession, context);
            dataset.printSchema();
        } else {
            if (StringUtils.isNotBlank(sql)) {
                dataset = sparkSession.sql(sql);
            }
            if (dataset != null) {
                dataset.show();
            }
        }
    }
}

package com.jaeng.adhesive.output;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.component.AbstractOutput;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class TextOutput extends AbstractOutput {

    private boolean append;
    private String path;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.append = MapUtils.getBooleanValue(conf, "append", false);
        this.path = MapUtils.getString(conf, "path", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null && StringUtils.isNotBlank(path)) {
            if (append) {
                dataset.write().mode(SaveMode.Append).text(path);
            } else {
                dataset.write().text(path);
            }
        }
    }
}

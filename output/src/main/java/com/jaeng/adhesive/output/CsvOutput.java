package com.jaeng.adhesive.output;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.component.AbstractOutput;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class CsvOutput extends AbstractOutput {

    private boolean append;
    private boolean header;
    private String delimiter;
    private String path;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        append = MapUtils.getBooleanValue(conf, "append", false);
        header = MapUtils.getBooleanValue(conf, "header", false);
        delimiter = MapUtils.getString(conf, "delimiter", ",");
        path = MapUtils.getString(conf, "path", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null) {
            DataFrameWriter write = dataset.write();

            if (append) {
                write.format("csv").mode(SaveMode.Append);
            } else {
                write.format("csv");
            }
            write.option("delimiter", delimiter).option("header", header);

            if (StringUtils.isNotEmpty(path)) {
                write.csv(path);
            }
        }
    }
}

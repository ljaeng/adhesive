package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
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
public class CsvSink extends AbstractSink {

    private boolean append;
    private boolean overwrite;
    private boolean header;
    private String delimiter;
    private String path;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        append = MapUtils.getBooleanValue(conf, "append", false);
        this.overwrite = MapUtils.getBooleanValue(conf, "overwrite", false);
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
            } else if (overwrite) {
                write.format("csv").mode(SaveMode.Overwrite);
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

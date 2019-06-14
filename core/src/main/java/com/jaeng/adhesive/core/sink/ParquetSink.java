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
public class ParquetSink extends AbstractSink {

    // 512 MB
    private static int DEFAULT_BLOCK_SIZE = 512 * 1024 * 1024;

    private String partitionFiled;
    private boolean append;
    private int blockSize;
    private String path;


    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.partitionFiled = MapUtils.getString(conf, "partitionFiled", "");
        this.append = MapUtils.getBooleanValue(conf, "append", false);
        this.blockSize = MapUtils.getIntValue(conf, "blockSize", DEFAULT_BLOCK_SIZE);
        this.path = MapUtils.getString(conf, "path", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);

        if (dataset != null && StringUtils.isNotEmpty(path)) {
            DataFrameWriter dataFrameWriter = dataset.write();
            if (append) {
                dataFrameWriter = dataFrameWriter.mode(SaveMode.Append);
            }

            if (StringUtils.isNotBlank(partitionFiled)) {
                dataFrameWriter = dataFrameWriter.partitionBy(this.partitionFiled);
            }

            dataFrameWriter.option("parquet.block.size", blockSize)
                    .parquet(path);

        }
    }
}

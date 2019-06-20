package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class TextSink extends AbstractSink {

    private boolean append;
    private String path;
    private String delimiter;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.append = MapUtils.getBooleanValue(conf, "append", false);
        this.path = MapUtils.getString(conf, "path", "");
        this.delimiter = MapUtils.getString(conf, "delimiter", "\t");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null && StringUtils.isNotBlank(path)) {
            if (dataset.schema().size() > 1) {
                dataset = dataset.mapPartitions((MapPartitionsFunction<Row, String>) rowIterator -> {
                    List<String> result = new LinkedList<>();
                    if (rowIterator != null) {
                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            String[] fieldNames = row.schema().fieldNames();
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < fieldNames.length; i++) {
                                String field = fieldNames[i];
                                if (i > 0) {
                                    sb.append(delimiter);
                                }
                                sb.append(row.getAs(field).toString());
                            }
                            result.add(sb.toString());
                        }
                    }
                    return result.iterator();
                }, Encoders.STRING());
            }
            if (append) {
                dataset.write().mode(SaveMode.Append).text(path);
            } else {
                dataset.write().text(path);
            }
        }
    }
}

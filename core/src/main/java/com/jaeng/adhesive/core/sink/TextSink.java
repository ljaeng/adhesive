package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;

import java.util.*;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class TextSink extends AbstractSink {

    private boolean append;
    private boolean overwrite;
    private String path;
    private String delimiter;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.append = MapUtils.getBooleanValue(conf, "append", false);
        this.overwrite = MapUtils.getBooleanValue(conf, "overwrite", false);
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
                                Object value = row.getAs(field);
                                sb.append(value);
                            }
                            result.add(sb.toString());
                        }
                    }
                    return result.iterator();
                }, Encoders.STRING());
            }
            if (append) {
                dataset.write().mode(SaveMode.Append).text(path);
            } else if (overwrite) {
                dataset.write().mode(SaveMode.Overwrite).text(path);
            } else {
                dataset.write().text(path);
            }
        }
    }
}

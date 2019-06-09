package com.jaeng.adhesive.input;

import com.jaeng.adhesive.core.component.AbstractInput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class ParquetInput extends AbstractInput {
    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        Dataset dataset = sparkSession.read().parquet(path.split(","));
        super.registerTable(context, dataset);
    }
}

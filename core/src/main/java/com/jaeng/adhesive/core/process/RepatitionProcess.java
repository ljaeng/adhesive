package com.jaeng.adhesive.core.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class RepatitionProcess extends AbstractProcess {

    private int partiton;
    private int coalesce;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        partiton = MapUtils.getIntValue(conf, "partiton", 0);
        coalesce = MapUtils.getIntValue(conf, "coalesce", 0);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        String sql = String.format("Select * from %s", table);
        Dataset dataSet = null;

        if (partiton > 0) {
            dataSet = sparkSession.sql(sql)
                    .repartition(partiton);
        } else if (coalesce > 0) {
            dataSet = sparkSession.sql(sql)
                    .coalesce(coalesce);
        }
        if (dataSet != null) {
            super.registerTable(context, dataSet);
        }
    }
}

package com.jaeng.adhesive.core.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class KuduSource extends AbstractSource {

    private String master;
    private String kuduTable;
    private String table;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.master = conf.getString("master");
        this.kuduTable = conf.getString("kuduTable");
        this.table = conf.getString("table");
        if (StringUtils.isEmpty(table)) {
            String arry[] = kuduTable.split("\\.");
            this.table = arry[arry.length - 1];
        }
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        Dataset dataset = sparkSession.sqlContext().read()
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", master)
                .option("kudu.table", kuduTable)
                .load();

        super.registerTable(context, dataset);

    }
}

package com.jaeng.adhesive.core.sink;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.InputConstant;
import com.jaeng.adhesive.common.enums.JdbcEnum;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class JdbcSink extends AbstractSink {

    private String url;
    private String userName;
    private String password;
    private String jdbcTable;
    private String jdbcType;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        url = MapUtils.getString(this.conf, "url", "");
        password = MapUtils.getString(this.conf, "password", "");
        userName = MapUtils.getString(this.conf, "userName", "");
        jdbcTable = MapUtils.getString(this.conf, "jdbcTable", "");
        jdbcType = MapUtils.getString(this.conf, "jdbcType", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", this.userName);
        connectionProperties.put("password", this.password);

        if (JdbcEnum.POSTGRE.getType().equals(jdbcType)) {
            connectionProperties.put("driver", JdbcEnum.POSTGRE.getDriver());
        } else if (JdbcEnum.HIVE.getType().equals(jdbcType)) {
            connectionProperties.put("driver", JdbcEnum.HIVE.getDriver());
        } else if (JdbcEnum.MYSQL.getType().equals(jdbcType)) {
            connectionProperties.put("driver", JdbcEnum.MYSQL.getDriver());
        }
        Dataset dataset = super.getProcessDataSet(sparkSession, context);

        dataset.write()
                .mode(SaveMode.Append)
                .jdbc(this.url, this.jdbcTable, connectionProperties);
    }
}

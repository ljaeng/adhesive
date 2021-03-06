package com.jaeng.adhesive.core.source;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.InputConstant;
import com.jaeng.adhesive.common.enums.JdbcEnum;
import com.jaeng.adhesive.common.util.JdbcConnect;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class JdbcSource extends AbstractSource {

    private String url;
    private String userName;
    private String password;
    private String jdbcTable;
    private String jdbcType;
    private boolean local;
    private String sql;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        url = MapUtils.getString(this.conf, "url", "");
        userName = MapUtils.getString(this.conf, "userName", "");
        password = MapUtils.getString(this.conf, "password", "");
        jdbcTable = MapUtils.getString(this.conf, "jdbcTable", "");
        jdbcType = MapUtils.getString(this.conf, "jdbcType", JdbcEnum.HIVE.getDriver());
        local = MapUtils.getBooleanValue(this.conf, "local", false);
        sql = MapUtils.getString(this.conf, "sql", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        Dataset<Row> dataset = null;
        if (local) {
            JdbcConnect jdbcConnect = null;
            try {
                jdbcConnect = JdbcConnect.initJdbcConnect(url, userName, password, jdbcType);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (Objects.nonNull(jdbcConnect)) {
                try {
                    List<String> list = jdbcConnect.query(sql, new JdbcConnect.ResultSetToJsonStr());
                    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
                    dataset = sparkSession.read()
                            .json(jsc.parallelize(list));
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    jdbcConnect.release();
                }
            }

        } else {
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", this.userName);
            connectionProperties.put("password", this.password);

            if (JdbcEnum.POSTGRE.getType().equals(jdbcType)) {
                connectionProperties.put("driver", JdbcEnum.POSTGRE.getDriver());
            } else if (JdbcEnum.MYSQL.getType().equals(jdbcType)) {
                connectionProperties.put("driver", JdbcEnum.MYSQL.getDriver());
            } else if (JdbcEnum.HIVE.getType().equals(jdbcType)) {
                connectionProperties.put("driver", JdbcEnum.HIVE.getDriver());
            }
            dataset = sparkSession
                    .read()
                    .jdbc(this.url, this.jdbcTable, connectionProperties);
        }


        super.registerTable(context, dataset);
    }
}

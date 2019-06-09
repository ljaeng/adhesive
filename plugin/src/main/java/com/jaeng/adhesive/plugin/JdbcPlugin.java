package com.jaeng.adhesive.plugin;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.PluginConstant;
import com.jaeng.adhesive.common.util.JdbcConnect;
import com.jaeng.adhesive.core.component.AbstractPlugin;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JdbcPlugin extends AbstractPlugin {

    private String url;
    private String user;
    private String password;
    private String type;
    private String action;
    private String condition;
    private String sql;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.type = MapUtils.getString(conf, "type", PluginConstant.PLUGIN_JDBC_HIVE_DRIVER);
        this.url = MapUtils.getString(conf, "url", "");
        this.user = MapUtils.getString(conf, "user", "");
        this.password = MapUtils.getString(conf, "password", "");
        this.sql = MapUtils.getString(conf, "sql", null);
        this.action = MapUtils.getString(conf, "action", "update");
        this.condition = MapUtils.getString(conf, "condition", null);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        JdbcConnect jdbcConnect = null;
        try {
            if (PluginConstant.PLUGIN_JDBC_HIVE_TYPE.equals(type)) {
                jdbcConnect = new JdbcConnect(url, user, password, PluginConstant.PLUGIN_JDBC_MYSQL_DRIVER);
            } else if (PluginConstant.PLUGIN_JDBC_HIVE_TYPE.equals(type)) {
                jdbcConnect = new JdbcConnect(url, user, password, PluginConstant.PLUGIN_JDBC_HIVE_DRIVER);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (jdbcConnect != null) {
            if (PluginConstant.PLUGIN_JDBC_ACTION_UPDATE.equals(action)) {
                try {
                    jdbcConnect.update(sql, null);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (jdbcConnect != null) {
                        jdbcConnect.release();
                    }
                }
            }
        }
    }

    @Override
    public String getRegisterName() {
        return "jdbc";
    }
}

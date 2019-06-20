package com.jaeng.adhesive.core.plugin;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.PluginConstant;
import com.jaeng.adhesive.common.enums.JdbcEnum;
import com.jaeng.adhesive.common.util.BeetlTemplateUtil;
import com.jaeng.adhesive.common.util.JdbcConnect;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JdbcPlugin extends AbstractPlugin {

    private Logger logger = LoggerFactory.getLogger(JdbcPlugin.class);

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

        this.type = MapUtils.getString(conf, "type", JdbcEnum.HIVE.getDriver());
        this.url = MapUtils.getString(conf, "url", "");
        this.user = MapUtils.getString(conf, "user", "");
        this.password = MapUtils.getString(conf, "password", "");
        this.sql = MapUtils.getString(conf, "sql", null);
        this.action = MapUtils.getString(conf, "action", "update");
        this.condition = MapUtils.getString(conf, "condition", null);
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        if (!StringUtils.isEmpty(condition) && !BeetlTemplateUtil.eval(context, condition)) {

            logger.info("不符合条件，不执行!");
        } else {
            JdbcConnect jdbcConnect = null;
            try {
                if (JdbcEnum.HIVE.getType().equals(type)) {
                    jdbcConnect = new JdbcConnect(url, user, password, JdbcEnum.HIVE.getDriver());
                } else if (JdbcEnum.MYSQL.getType().equals(type)) {
                    jdbcConnect = new JdbcConnect(url, user, password, JdbcEnum.MYSQL.getDriver());
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
    }
}

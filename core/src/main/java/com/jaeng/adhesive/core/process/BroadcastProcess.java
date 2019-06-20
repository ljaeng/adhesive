package com.jaeng.adhesive.core.process;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.ProcessConstant;
import com.jaeng.adhesive.common.enums.JdbcEnum;
import com.jaeng.adhesive.common.util.JdbcConnect;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * @author lizheng
 * @date 2019/6/19
 */
public class BroadcastProcess extends AbstractProcess {

    private String sql;
    private String broadcastName;
    private String broadcastType;
    private String keyFiled;
    private String valueFiled;
    private String type;
    private String jdbcType;
    private String url;
    private String userName;
    private String password;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        this.sql = MapUtils.getString(conf, "sql", "");
        this.broadcastName = MapUtils.getString(conf, "broadcastName", "");
        this.broadcastType = MapUtils.getString(conf, "broadcastType", "");
        this.keyFiled = MapUtils.getString(conf, "keyFiled", "");
        this.valueFiled = MapUtils.getString(conf, "valueFiled", "");
        this.type = MapUtils.getString(conf, "type", "");
        this.jdbcType = MapUtils.getString(conf, "jdbcType", "");
        url = MapUtils.getString(this.conf, "url", "");
        password = MapUtils.getString(this.conf, "password", "");
        userName = MapUtils.getString(this.conf, "userName", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Object broadcast = null;
        if (ProcessConstant.BROADCAST_PROCESS_BROADCAST_MAP_TYPE.equals(broadcastType)) {
            broadcast = new HashMap<String, Object>(128);
        } else if (ProcessConstant.BROADCAST_PROCESS_BROADCAST_MAP_TYPE.equals(broadcastType)) {
            broadcast = new LinkedList<String>();
        }

        if (ProcessConstant.BROADCAST_PROCESS_DATASET_TYPE.equals(type)) {
            Dataset<Row> dataset = (Dataset<Row>) super.getProcessDataSet(sparkSession, context);
            if (Objects.nonNull(dataset)) {
                List<Row> rows = dataset.collectAsList();
            }
        } else if (ProcessConstant.BROADCAST_PROCESS_JDBC_TYPE.equals(type)) {
            String driver = null;
            if (JdbcEnum.MYSQL.getType().equals(jdbcType)) {
                driver = JdbcEnum.MYSQL.getDriver();
            } else if (JdbcEnum.HIVE.getType().equals(jdbcType)) {
                driver = JdbcEnum.HIVE.getDriver();
            } else if (JdbcEnum.POSTGRE.getType().equals(jdbcType)) {
                driver = JdbcEnum.POSTGRE.getDriver();
            }
            if (StringUtils.isNotEmpty(driver)) {
                JdbcConnect jdbcConnect = null;
                try {
                    jdbcConnect = new JdbcConnect(url, userName, password, driver);
                    List<Map<String, Object>> list = jdbcConnect.query(sql);
                    for (Map<String, Object> map : list) {
                        if (broadcast instanceof Map) {
                            if (map.containsKey(keyFiled) && map.containsKey(valueFiled)) {
                                ((Map<String, Object>) broadcast).put(map.get(keyFiled).toString(), map.get(valueFiled));
                            }
                        } else if (broadcast instanceof List) {
                            if (map.containsKey(valueFiled)) {
                                ((List<String>) broadcast).add(map.get(valueFiled).toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (Objects.nonNull(jdbcConnect)) {
                        jdbcConnect.release();
                    }
                }
            }
        }
    }
}

package com.jaeng.adhesive.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.PluginConstant;
import com.jaeng.adhesive.common.constant.ProcessConstant;
import com.jaeng.adhesive.common.util.JdbcConnect;
import com.jaeng.adhesive.common.util.NumberUtil;
import com.jaeng.adhesive.core.component.AbstractProcess;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JoinJdbcProcess extends AbstractProcess {

    private String sql;
    private String joinSql;
    private String joinField;
    private String jdbcUrl;
    private String type;
    private String user;
    private String password;
    private int batch;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.sql = MapUtils.getString(conf, "sql");

        this.joinSql = MapUtils.getString(conf, "joinSql");
        this.joinField = MapUtils.getString(conf, "joinField");
        this.jdbcUrl = MapUtils.getString(conf, "jdbcUrl");
        this.batch = MapUtils.getIntValue(conf, "batch", 100);

        this.type = MapUtils.getString(conf, "type", PluginConstant.PLUGIN_JDBC_HIVE_DRIVER);
        this.user = MapUtils.getString(conf, "user", "");
        this.password = MapUtils.getString(conf, "password", "");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset;
        if (StringUtils.isNotEmpty(sql)) {
            dataset = sparkSession.sql(sql);
        } else {
            dataset = super.getProcessDataSet(sparkSession, context);
        }

        if (dataset != null) {
            Dataset result = dataset.mapPartitions((MapPartitionsFunction) input -> {
                JdbcConnect jdbcConnect = null;
                try {
                    if (ProcessConstant.PROCESS_JDBC_MYSQL_TYPE.equals(type)) {
                        jdbcConnect = new JdbcConnect(jdbcUrl, user, password, ProcessConstant.PROCESS_JDBC_MYSQL_DRIVER);
                    } else if (ProcessConstant.PROCESS_JDBC_HIVE_TYPE.equals(type)) {
                        jdbcConnect = new JdbcConnect(jdbcUrl, user, password, ProcessConstant.PROCESS_JDBC_HIVE_DRIVER);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                List<String> resultList = new LinkedList<>();
                if (jdbcConnect != null) {

                    List<Object> list = new LinkedList<>();

                    while (input.hasNext()) {

                        try {
                            Row row = (Row) input.next();
                            Object v = row.getAs(joinField);
                            if (v != null) {
                                list.add(v);
                            }
                            if (list.size() > 0 && list.size() % batch == 0) {
                                List<String> segResult = join(jdbcConnect, list);
                                if (segResult != null && segResult.size() > 0) {
                                    resultList.addAll(segResult);
                                }
                                list.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if (list.size() > 0) {
                        try {
                            List<String> segResult = join(jdbcConnect, list);
                            if (segResult != null && segResult.size() > 0) {
                                resultList.addAll(segResult);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    jdbcConnect.release();
                }
                return resultList.iterator();
            }, Encoders.STRING());
            super.registerTable(context, result);
        }
    }

    private List join(JdbcConnect jdbcConnect, List<Object> ids) {

        if (ids != null && ids.size() > 0) {

            String mergedIds = mergeIds(ids);

            String querySql = joinSql.replace("{ids}", mergedIds);


            try {
                List<Map<String, Object>> list = jdbcConnect.query(querySql);

                if (list != null && list.size() > 0) {

                    List<String> result = new LinkedList<>();

                    for (Map<String, Object> obj : list) {

                        result.add(JSON.toJSONString(obj));
                    }

                    return result;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return null;
    }

    private String mergeIds(List<Object> ids) {

        if (NumberUtil.isNum(ids)) {
            return StringUtils.join(ids, ",");
        } else {
            return "'" + StringUtils.join(ids, "','") + "'";
        }
    }
}

package com.jaeng.adhesive.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JdbcConnect {
    private static final Logger logger = LoggerFactory.getLogger(JdbcConnect.class);

    private Connection connection;

    public JdbcConnect(String jdbcUrl, String user, String password, String driver) throws Exception {
        logger.debug("新建数据库链接[{}]", jdbcUrl);
        Class.forName(driver);
        connection = DriverManager.getConnection(jdbcUrl, user, password);
    }

    public List<Map<String, Object>> query(String sql, List<Object> parameters) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            initStatementParam(parameters, statement);
            return resultSetToMap(statement.executeQuery());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
        return null;
    }


    public int update(String sql, List<Object> parameters) throws SQLException {

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            initStatementParam(parameters, statement);
            return statement.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
        }
        return 0;
    }

    private void initStatementParam(List<Object> parameters, PreparedStatement statement) throws SQLException {
        if (parameters != null) {
            for (int i = 0; i < parameters.size(); i++) {
                if (parameters.get(i) instanceof String[]) {
                    statement.setArray(i + 1, connection.createArrayOf("string", (String[]) parameters.get(i)));
                } else {
                    statement.setObject(i + 1, parameters.get(i));
                }
            }
        }
    }


    protected List<Map<String, Object>> resultSetToMap(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        if (resultSet != null) {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            List<String> columns = new ArrayList<>(rsmd.getColumnCount());
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columns.add(rsmd.getColumnName(i));
            }
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>(columns.size());
                for (String col : columns) {
                    row.put(col.toLowerCase(), resultSet.getObject(col));
                }
                data.add(row);
            }
        }
        return data;
    }

    public List<Map<String, Object>> query(String sql) throws SQLException {
        logger.info("query sql: {}", sql);
        java.util.Date date = new Date();
        Statement statement = null;
        List<Map<String, Object>> data = new ArrayList<>();
        try {
            statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(sql);
            data.addAll(resultSetToMap(resultSet));
            resultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
        logger.debug("finish query sql: [{}],  use time:[{}]", sql, (System.currentTimeMillis() - date.getTime()));
        return data;
    }


    /**
     * 执行完成，关闭链接
     */
    public void release() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}

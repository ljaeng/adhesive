package com.jaeng.adhesive.common.enums;

/**
 * @author lizheng
 * @date 2019/6/19
 */
public enum JdbcEnum {

    MYSQL("mysql", "com.mysql.jdbc.Driver"),
    HIVE("hive", "org.apache.hive.jdbc.HiveDriver"),
    POSTGRE("postgrepostgre", "org.postgresql.Driver"),
    ;

    private String type;
    private String driver;

    JdbcEnum(String type, String driver) {
        this.type = type;
        this.driver = driver;
    }

    public String getType() {
        return type;
    }

    public String getDriver() {
        return driver;
    }
}

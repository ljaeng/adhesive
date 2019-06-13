package com.jaeng.adhesive.common.constant;

/**
 * Input的静态常量
 *
 * @author lizheng
 * @date 2019/6/6
 */
public class InputConstant {

    /**
     * -----------------------------CSV Start-----------------------------------------------
     */
    /**
     * CSV列的Schema
     */
    public static final String INPUT_CSV_CONF_FIELD = "field";

    /**
     * CSV列的Header
     */
    public static final String INPUT_CSV_HEADER = "header";
    /**
     * CSV列的分隔符
     */
    public static final String INPUT_CSV_DELIMITER = "delimiter";
    /**
     * -----------------------------CSV End-----------------------------------------------
     */

    /**
     * -----------------------------JDBC Start-----------------------------------------------
     */
    public static final String INPUT_JDBC_MYSQL_TYPE = "mysql";
    public static final String INPUT_JDBC_POSRGRESQL_TYPE = "postgre";

    public static final String INPUT_POSRGRESQL_DRIVER = "org.postgresql.Driver";
    public static final String INPUT_MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    /**
     * -----------------------------JDBC End-----------------------------------------------
     */

}

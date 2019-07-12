package com.jaeng.adhesive.core.udf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author lizheng
 * @date 2019/6/20
 */
public class DateFormatUdf extends AbstractUdf implements UDF3<String, String, String, String> {

    @Override
    public String call(String preDateStr, String prePattern, String pattern) throws Exception {
        DateFormat preDateFormat = new SimpleDateFormat(prePattern);
        DateFormat dateFormat = new SimpleDateFormat(pattern);

        try {
            Date preDate = preDateFormat.parse(preDateStr);
            return dateFormat.format(preDate);
        } catch (Exception e) {
            System.out.println(String.format("[DateFormatUdf] 转换错误, preDateStr:[%s],prePattern:[%s],pattern:[%s]", preDateStr, preDateFormat, pattern));
            throw e;
        }
    }

    @Override
    public String getRegisterName() {
        return "date_format2";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }

    @Override
    protected String use_desc() {
        return "date_format2(20190501, yyyyMMdd, yyyy-MM-dd), 返回值: 2019-05-01; 第一个参数:需要转换的日期字符串, 第二个参数:当前的日期格式, 第三个参数:需要转换的日期格式";
    }
}

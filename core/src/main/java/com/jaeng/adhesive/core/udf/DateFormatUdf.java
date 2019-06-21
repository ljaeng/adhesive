package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.core.api.Registerable;
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
public class DateFormatUdf implements UDF3<String, String, String, String>, Registerable {

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
}

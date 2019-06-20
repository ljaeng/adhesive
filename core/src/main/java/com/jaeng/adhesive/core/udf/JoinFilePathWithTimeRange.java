package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.common.constant.Constant;
import com.jaeng.adhesive.core.api.Registerable;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.text.ParseException;
import java.util.Date;

/**
 * @author lizheng
 * @date 2019/6/17
 */
public class JoinFilePathWithTimeRange implements UDF3<String, String, String, String>, Registerable {


    @Override
    public String call(String preStr, String startDate, String endDate) throws Exception {
        try {
            StringBuilder timeRangeSb = new StringBuilder(preStr);
            timeRangeSb.append("{");
            long startTime = Constant.SIMPLE_DATE_FORMAT_YYYYMMDD.parse(startDate).getTime();
            long endTime = Constant.SIMPLE_DATE_FORMAT_YYYYMMDD.parse(endDate).getTime();

            long days = (endTime - startTime) / Constant.ONE_DAT_TIME;
            for (long i = 0; i < days; i++) {
                String date = Constant.SIMPLE_DATE_FORMAT_YYYYMMDD.format(new Date(startTime + i * Constant.ONE_DAT_TIME));
                if (i > 0) {
                    timeRangeSb.append(",");
                }
                timeRangeSb.append(date);
            }
            timeRangeSb.append("}");
            return timeRangeSb.toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String getRegisterName() {
        return "join_file_path_with_time_range";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }
}

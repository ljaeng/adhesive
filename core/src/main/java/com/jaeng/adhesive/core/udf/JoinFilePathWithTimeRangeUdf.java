package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.common.constant.Constant;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.text.ParseException;
import java.util.Date;

/**
 * @author lizheng
 * @date 2019/6/17
 */
public class JoinFilePathWithTimeRangeUdf extends AbstractUdf implements UDF3<String, String, String, String> {


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
            throw e;
        }
    }

    @Override
    public String getRegisterName() {
        return "join_file_path_with_time_range";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }

    @Override
    protected String use_desc() {
        return "join_file_path_with_time_range(yyyyMM, 20190501, 20190503), 返回值: {20190501,20190502}; 第一个参数:当前的日期格式, 第二个参数:开始时间, 第三个参数:结束时间";
    }
}

package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.core.api.Registerable;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author lizheng
 * @date 2019/6/17
 */
public class TextSplitUdf implements UDF3<String, String, Integer, String>, Registerable {

    /**
     * @param line  需要切分的字符串
     * @param regex 切分规则
     * @param index 结果索引
     * @return
     * @throws Exception
     */
    @Override
    public String call(String line, String regex, Integer index) throws Exception {

        String[] split = line.split(regex);
        if (split.length > index) {
            return split[index];
        }
        return null;
    }

    @Override
    public String getRegisterName() {
        return "text_split_value";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }
}

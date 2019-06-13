package com.jaeng.adhesive.udf;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.Registerable;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * 根据Key获取Json字符串中的Value
 *
 * @author lizheng
 * @date 2019/6/9
 */
public class JsonFieldUdf implements UDF2<String, String, String>, Registerable {

    @Override
    public String call(String jsonStr, String key) throws Exception {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSONObject.parseObject(jsonStr);
        } catch (Exception e) {
            System.out.println(String.format("[JsonFieldUdf] 解析Json字符串错误.[%jsonStr]", key));
        }
        if (jsonObject != null) {
            return jsonObject.getString(key);
        }
        return null;
    }

    @Override
    public String getRegisterName() {
        return "get_json_field";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }
}

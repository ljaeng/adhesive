package com.jaeng.adhesive.core.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * 根据Key获取Json字符串中的Value
 *
 * @author lizheng
 * @date 2019/6/9
 */
public class JsonFieldUdf extends AbstractUdf implements UDF2<String, String, String> {

    @Override
    public String call(String jsonStr, String key) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            return jsonObject.getString(key);
        } catch (Exception e) {
            System.out.println(String.format("[JsonFieldUdf] 解析Json字符串错误.[%s]", key));
            throw e;
        }
    }

    @Override
    public String getRegisterName() {
        return "get_json_field";
    }

    @Override
    public DataType getDataType() {
        return DataTypes.StringType;
    }

    @Override
    protected String use_desc() {
        return "get_json_field({\"key\": 1}, key), 返回值: 1; 第一个参数:JSON字符串, 第二个参数:要取的Key";
    }
}

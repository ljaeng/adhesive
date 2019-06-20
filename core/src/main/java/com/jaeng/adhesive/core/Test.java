package com.jaeng.adhesive.core;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.util.PatternUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class Test {

    public static void main(String[] args) {


//        SparkSession sparkSession = new SparkSession.Builder()
//                .master("local")
//                .appName("test")
//                .getOrCreate();
//
//        Dataset<Row> dataset = sparkSession.read()
//                .json("/Users/jaeng/Developments/OwnProject/adhesive/file/device_price")
//                .toDF();
//
//        Dataset<Row> dataset1 = dataset.map(new MapFunction<Row, JSONObject>() {
//            @Override
//            public JSONObject call(Row row) throws Exception {
//                long price = row.getLong(1);
//                JSONObject jsonObject = new JSONObject();
//                String[] fieldNames = row.schema().fieldNames();
//                for (String fieldName : fieldNames) {
//                    jsonObject.put(fieldName, row.getAs(fieldName));
//                }
//                jsonObject.put("price_", price / 10);
//                return jsonObject;
//            }
//        }, Encoders.kryo(JSONObject.class))
//                .toDF();
//
//        dataset1.show();
//
////        dataset1.toDF().show();
////
////        dataset1.toJSON().toDF().show();
//
//        sparkSession.stop();

        String regex = "create +broadcast +(.*?) *as +(.*?) *. *`(.*)` *(.*)";
        System.out.println(PatternUtil.findValueByRegex(regex, "create table aa as select 1 as a;"));
        System.out.println(PatternUtil.findValueByRegex(regex, "create broadcast aa as select 1 as a;"));
        System.out.println(PatternUtil.findValueByRegex(regex, "create broadcast aa as jdbc.``;"));
    }

}
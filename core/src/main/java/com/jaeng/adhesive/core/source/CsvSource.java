package com.jaeng.adhesive.core.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.InputConstant;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class CsvSource extends AbstractSource {

    private boolean split;
    private String splitStr;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);
        split = MapUtils.getBooleanValue(conf, "split", false);
        splitStr = MapUtils.getString(conf, "splitStr", ",");
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        DataFrameReader dataFrameReader = sparkSession.read().format("csv");

        StructType structType = null;

        if (this.conf != null) {
            if (this.conf.containsKey(InputConstant.INPUT_CSV_HEADER)) {
                dataFrameReader.option(InputConstant.INPUT_CSV_HEADER, MapUtils.getBooleanValue(this.conf, InputConstant.INPUT_CSV_HEADER, false));
            }
            if (this.conf.containsKey(InputConstant.INPUT_CSV_DELIMITER)) {
                dataFrameReader.option(InputConstant.INPUT_CSV_DELIMITER, MapUtils.getString(this.conf, InputConstant.INPUT_CSV_DELIMITER, ","));
            }

            if (this.conf.containsKey(InputConstant.INPUT_CSV_CONF_FIELD)) {

                List<StructField> fields = new ArrayList<>();

                JSONArray jsonArray = JSONArray.parseArray(this.conf.getString(InputConstant.INPUT_CSV_CONF_FIELD));

                for (int i = 0; i < jsonArray.size(); i++) {

                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    String type = jsonObject.getString("type");

                    if ("int".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.IntegerType, true));
                    } else if ("long".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.LongType, true));
                    } else if ("float".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.FloatType, true));
                    } else if ("double".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.DoubleType, true));
                    } else if ("boolean".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.BooleanType, true));
                    } else if ("short".equals(type)) {

                        fields.add(DataTypes.createStructField(name, DataTypes.ShortType, true));
                    } else if ("date".equals(type)) {
                        fields.add(DataTypes.createStructField(name, DataTypes.DateType, true));
                    } else if ("timestamp".equals(type)) {
                        fields.add(DataTypes.createStructField(name, DataTypes.TimestampType, true));
                    } else {
                        fields.add(DataTypes.createStructField(name, DataTypes.StringType, true));
                    }
                }
                structType = DataTypes.createStructType(fields);
            }
        }

        if (structType != null) {
            dataFrameReader = dataFrameReader.schema(structType);
        }
        Dataset dataset;
        if (split) {
            dataset = dataFrameReader.load(path.split(splitStr));
        } else {
            dataset = dataFrameReader.load(path);
        }

        super.registerTable(context, dataset);
    }
}

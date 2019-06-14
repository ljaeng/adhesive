package com.jaeng.adhesive.core.task;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.enums.ComponentTypeEnum;
import com.jaeng.adhesive.common.util.PatternUtil;
import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Objects;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class SetTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        if (line.contains(" as ")) {
            String regex = "set +(.*?) *as *(.*)";
            String key = PatternUtil.getValueByRegex(regex, line, 1);
            String sql = PatternUtil.getValueByRegex(regex, line, 2);

            Component component = null;
            try {
                component = job.getComponent(ComponentTypeEnum.PROCESS.getType(), "sql");
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (component != null) {
                JSONObject conf = new JSONObject();
                conf.put("dataSetName", key);
                conf.put("sql", sql);
                component.setConf(conf);
                component.run(sparkSession, context);

                Object result = context.get(key);
                if (Objects.nonNull(result)) {
                    Dataset dataset = (Dataset) result;
                    Row[] rows = (Row[]) dataset.take(1);
                    if (rows != null && rows.length > 0) {
                        for (Row row : rows) {
                            String fields[] = row.schema().fieldNames();
                            for (String field : fields) {
                                if (fields.length > 1) {
                                    context.put(key + "." + field, row.getAs(field));
                                } else {
                                    context.put(key, row.getAs(field));
                                }
                            }
                        }
                    }
                }
            }
        } else {
            String regex = "set +(.*?) *= *([^ ;]+).*";
            String key = PatternUtil.getValueByRegex(regex, line, 1);
            String value = PatternUtil.getValueByRegex(regex, line, 2);
            context.put(key, value);
        }
    }
}

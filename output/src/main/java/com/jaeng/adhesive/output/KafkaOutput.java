package com.jaeng.adhesive.output;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.util.CollectionUtil;
import com.jaeng.adhesive.common.util.KafkaUtil;
import com.jaeng.adhesive.core.component.AbstractOutput;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * @author lizheng
 * @date 2019/6/8
 */
public class KafkaOutput extends AbstractOutput {

    private String broker;
    private String topic;
    private List<String> fields;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.broker = MapUtils.getString(conf, "broker", "");
        this.topic = MapUtils.getString(conf, "topic", "");
        String fieldStr = MapUtils.getString(conf, "fields");
        if (StringUtils.isNotEmpty(fieldStr)) {
            this.fields = Arrays.asList(fieldStr.split(","));
        } else {
            fields = Collections.EMPTY_LIST;
        }
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset = super.getProcessDataSet(sparkSession, context);
        if (dataset != null) {
            dataset.foreachPartition(msgIterator -> {
                if (msgIterator != null) {
                    KafkaProducer producer = KafkaUtil.getProducer(broker);

                    while (msgIterator.hasNext()) {
                        try {
                            Row row = (Row) msgIterator.next();

                            int dataLength = fields.size() >= 0 ? fields.size() : row.length();
                            Map<String, Object> dataMap = new HashMap<>(CollectionUtil.initSize(dataLength, CollectionUtil.HASH_MAP_DEFAULT_LOAD_FACTOR));
                            for (String name : row.schema().fieldNames()) {
                                if (fields.size() == 0 || fields.contains(name)) {
                                    dataMap.put(name, row.getAs(name));
                                }
                            }
                            String msg = JSONObject.toJSONString(dataMap);

                            ProducerRecord producerRecord = new ProducerRecord(topic, msg);
                            producer.send(producerRecord);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    producer.close();
                }
            });
        }
    }
}

package com.jaeng.adhesive.process;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.util.NumberUtil;
import com.jaeng.adhesive.core.component.AbstractProcess;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class JoinMongoProcess extends AbstractProcess {

    private Logger logger = LoggerFactory.getLogger(JoinMongoProcess.class);

    private String mongoUrl;
    private String mongoDatabase;
    private String mongoCollection;
    private String joinField;
    private List<String> mongoResultField = new ArrayList<>();

    private int batch;
    private String sql;
    private String joinSql;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.mongoUrl = MapUtils.getString(conf, "url");
        this.mongoDatabase = MapUtils.getString(conf, "database");
        this.joinField = MapUtils.getString(conf, "joinField");
        this.mongoCollection = MapUtils.getString(conf, "collection");
        this.joinSql = MapUtils.getString(conf, "joinSql");
        this.sql = MapUtils.getString(conf, "sql");
        this.batch = MapUtils.getIntValue(conf, "batch", 100);

        String mongoResultFieldStr = MapUtils.getString(conf, "mongoResultField");

        if (StringUtils.isNotBlank(mongoResultFieldStr)) {
            String fields[] = mongoResultFieldStr.split(",");
            for (String field : fields) {
                if (StringUtils.isNotBlank(field.trim()) && !mongoResultField.contains(field.trim())) {
                    mongoResultField.add(field.trim());
                }
            }
        }
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        Dataset dataset;
        if (StringUtils.isNotEmpty(sql)) {
            dataset = sparkSession.sql(sql);
        } else {
            dataset = super.getProcessDataSet(sparkSession, context);
        }

        if (dataset != null) {
            Dataset result = dataset.mapPartitions((MapPartitionsFunction) input -> {
                MongoClient mongoClient = null;
                MongoDatabase database = null;
                MongoCollection<Document> collection = null;
                List<String> resultJsonList = new LinkedList<>();
                try {
                    mongoClient = new MongoClient(new MongoClientURI(mongoUrl));
                    database = mongoClient.getDatabase(mongoDatabase);
                    collection = database.getCollection(mongoCollection);

                    List<Object> joinFieldValues = new LinkedList<>();
                    while (input.hasNext()) {
                        try {
                            Row row = (Row) input.next();
                            Object v = row.getAs(joinField);
                            if (v != null) {
                                joinFieldValues.add(v);
                            }
                            if (joinFieldValues.size() > 0 && joinFieldValues.size() % batch == 0) {
                                resultJsonList.addAll(join(collection, joinFieldValues));
                                joinFieldValues.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (joinFieldValues.size() > 0) {
                        resultJsonList.addAll(join(collection, joinFieldValues));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (mongoClient != null) {
                        mongoClient.close();
                    }
                }
                return resultJsonList.iterator();
            }, Encoders.STRING());

            super.registerTable(context, result);
        }
    }

    private List<String> join(MongoCollection collection, List<Object> ids) {
        String mergedIds = mergeIds(ids);
        String queryJson = joinSql.replace("{ids}", mergedIds);
        BasicDBObject query = BasicDBObject.parse(queryJson);

        logger.info("queryJson " + queryJson);
        Iterator<Document> queryResult = collection.find(query).iterator();
        List<String> result = new ArrayList<>();
        while (queryResult.hasNext()) {
            try {
                JSONObject json = new JSONObject();
                Document resultItem = queryResult.next();
                for (String field : mongoResultField) {
                    if (resultItem.containsKey(field)) {
                        json.put(field, resultItem.get(field));
                    }
                }
                logger.info("resultJson " + resultItem.toJson());
                result.add(json.toJSONString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private String mergeIds(List<Object> ids) {
        if (NumberUtil.isNum(ids)) {
            return StringUtils.join(ids, ",");
        } else {
            return "'" + StringUtils.join(ids, "','") + "'";
        }
    }
}

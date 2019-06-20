package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.common.constant.Constant;
import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedList;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class ClearTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        String split[] = line.trim().split(" +");
        String action = split[1];
        if ("table".equals(action)) {
            sparkSession.sqlContext().clearCache();
        } else if ("cache".equals(action)) {
            LinkedList<Dataset> cacheDataset = (LinkedList<Dataset>) context.get(Constant.CACHE_DATASET_LIST);
            for (Dataset dataset : cacheDataset) {
                dataset.unpersist();
            }
            cacheDataset.clear();
        } else {
            LinkedList<Dataset> cacheDataset = (LinkedList<Dataset>) context.get(Constant.CACHE_DATASET_LIST);
            Dataset dataset = null;
            try {
                dataset = (Dataset) context.get(action);
                dataset.unpersist();
            } catch (Exception e) {

            }
            if (dataset != null) {
                cacheDataset.remove(dataset);
            }
        }
    }
}

package com.jaeng.adhesive.core.job;

import com.jaeng.adhesive.core.task.TaskUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Objects;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class FileJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(FileJob.class);

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {

        String commandFilePath = super.conf.getConfig();
        BufferedReader reader = null;
        String line = null;
        StringBuilder sqlSb = new StringBuilder();
        StringBuilder preSqlSb = new StringBuilder();
        try {
            reader = new BufferedReader(new FileReader(commandFilePath));
            System.out.println("---------------------------");
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isNotEmpty(line) && (line.startsWith("#") || line.startsWith("--"))) {
                    System.out.println("跳过: " + line);
                } else {
                    sqlSb.append(line);
                    preSqlSb.append(line);
                    if (line.trim().endsWith(";")) {
                        line = sqlSb.toString();
                        System.out.println(preSqlSb.toString() + "\n");
                        sqlSb = new StringBuilder();
                        preSqlSb = new StringBuilder();

                        long start = System.currentTimeMillis();
                        try {
                            TaskUtil.process(this, line, sparkSession, context);
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.info("错误信息: " + e.toString());
                        }
                        System.out.println("耗时: " + (System.currentTimeMillis() - start) + " ms");
                        System.out.println("\n---------------------------");
                    } else {
                        if (sqlSb.length() > 0) {
                            sqlSb.append(" ");
                            preSqlSb.append("\n");
                        }
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (Objects.nonNull(reader)) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }


}

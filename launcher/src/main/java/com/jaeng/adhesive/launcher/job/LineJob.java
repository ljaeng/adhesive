package com.jaeng.adhesive.launcher.job;

import com.jaeng.adhesive.core.component.AbstractJob;
import com.jaeng.adhesive.core.component.Task;
import com.jaeng.adhesive.launcher.task.TaskUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class LineJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(LineJob.class);

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        System.out.println("---------------------");
        System.out.println("进入交互模式");
        System.out.println("---------------------");

        Scanner reader = new Scanner(System.in);

        String line;
        StringBuilder sqlSb = new StringBuilder();
        boolean processFlag = true;
        while (processFlag) {
            while ((line = reader.nextLine()) != null) {
                line = line.trim();
                if (StringUtils.isNotEmpty(line) && (line.startsWith("#") || line.startsWith("--"))) {
                    logger.info("Skip [{}]", line);
                } else {
                    sqlSb.append(line).append("\n");
                    if (line.trim().endsWith(";")) {
                        String sql = sqlSb.toString();
                        sqlSb = new StringBuilder();

                        if (sql.startsWith("stop") || sql.startsWith("exit") || sql.startsWith("quit")) {
                            processFlag = false;
                            break;
                        }
                        long start = System.currentTimeMillis();
                        try {
                            TaskUtil.process(this, sql, sparkSession, context);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("错误信息： " + e.toString());
                        }
                        System.out.println("处理SQL: " + sqlSb.toString());
                        System.out.println("耗时: " + (System.currentTimeMillis() - start) + " ms");
                        System.out.println("\n---------------------------");
                    }
                }
            }
        }
    }
}

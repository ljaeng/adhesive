package com.jaeng.adhesive.core.job;

import com.jaeng.adhesive.core.task.TaskUtil;
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
        System.out.println("---------------------------");
        System.out.println("进入命令行模式");
        System.out.println("---------------------------");

        Scanner reader = new Scanner(System.in);

        String line;
        StringBuilder sqlSb = new StringBuilder();
        boolean processFlag = true;
        while (processFlag) {
            while ((line = reader.nextLine()) != null) {
                line = line.trim();
                if (StringUtils.isNotEmpty(line) && (line.startsWith("#") || line.startsWith("--"))) {
                    System.out.println("跳过: " + line);
                } else {
                    sqlSb.append(line);
                    if (line.trim().endsWith(";")) {
                        line = sqlSb.toString();
                        sqlSb = new StringBuilder();

                        if (line.startsWith("stop") || line.startsWith("exit") || line.startsWith("quit")) {
                            processFlag = false;
                            System.out.println("------------Bye------------");
                            System.out.println("---------------------------");
                            break;
                        }

                        long start = System.currentTimeMillis();
                        try {
                            TaskUtil.process(this, line, sparkSession, context);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("错误信息: " + e.toString());
                        }

                        System.out.println("耗时: " + (System.currentTimeMillis() - start) + " ms");
                        System.out.println("\n---------------------------");
                    } else {
                        sqlSb.append(" ");
                    }
                }
            }
        }
    }
}

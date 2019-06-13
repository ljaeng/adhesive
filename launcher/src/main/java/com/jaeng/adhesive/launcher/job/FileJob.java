package com.jaeng.adhesive.launcher.job;

import com.jaeng.adhesive.core.component.AbstractJob;
import jline.console.ConsoleReader;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class FileJob extends AbstractJob {

    private static final Logger logger = LoggerFactory.getLogger(FileJob.class);

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        System.out.println("---------------------");
        System.out.println("进入交互模式");

        ConsoleReader reader = null;
        try {
            reader = new ConsoleReader();
            String line = null;

            StringBuffer stringBuffer = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                String localLine = line.trim();
                if (StringUtils.isNotEmpty(localLine) && (localLine.startsWith("#") || localLine.startsWith("--"))) {
                    logger.info("Skip [{}]", line);
                } else {
                    stringBuffer.append(line).append("\n");
                    if (line.trim().endsWith(";")) {
                        long start = System.currentTimeMillis();
//                        try {
//                            process(this, stringBuffer.toString(), sparkSession, context);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                            System.out.println("错误信息： " + e.toString());
//                        }
                        logger.info("处理SQL:[{}]", stringBuffer.toString());
                        stringBuffer = new StringBuffer();
                        System.out.println("耗时: " + (System.currentTimeMillis() - start) + " ms");
                        System.out.println("\n---------------------------");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

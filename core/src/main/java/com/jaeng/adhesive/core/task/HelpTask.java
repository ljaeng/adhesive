package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class HelpTask extends AbstractTask {

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        StringBuilder helpSb = new StringBuilder();

        helpSb.append("\n");
        helpSb.append("设置变量:\n");
        helpSb.append("\tset a = 1;\n");
        helpSb.append("\tset b as select unix_timestamp();\n");
        helpSb.append("\n");

        helpSb.append("查看变量:\n");
        helpSb.append("\tget a;\n");
        helpSb.append("\tselect ${a};\n");
        helpSb.append("\n");

        helpSb.append("创建表:\n");
        helpSb.append("\tcreate table 表名 as 类型.`{json配置}` | `路径`;\n");
        helpSb.append("\tcreate table tableName as text.`/path`;\n");
        helpSb.append("\tcreate table tableName as parquet.`/path`;\n");
        helpSb.append("\tcreate table tableName as jdbc.`{\"url\":\"\", \"userName\":\"\",\"password\":\"\",\"jdbcTable\":\"\",\"jdbcType\":\"\"}`;\n");
        helpSb.append("\tcreate table tableName as mongo.`{\"url\":\"\", \"database\":\"\",\"collection\":\"\"}`;\n");
        helpSb.append("\tcreate table table2 as select * from table1;\n");
        helpSb.append("\tcreate table table2 as select *,${a} from table1;引用变量 a \n");
        helpSb.append("\n");

        helpSb.append("缓存:\n");
        helpSb.append("\tset cache = true;表示下一个SQL数据会缓存\n");
        helpSb.append("\n");

        helpSb.append("清除缓存:\n");
        helpSb.append("\tclear table;清除所有缓存\n");
        helpSb.append("\tclear cache;清除所有缓存\n");
        helpSb.append("\n");

        helpSb.append("数据输出:\n");
        helpSb.append("\tinsert 类型.`{json配置}` | `路径`\n");
        helpSb.append("\tinsert parquet.`/path`;保存到HDFS\n");
        helpSb.append("\tinsert mysql.`{\"url\":\"\", \"userName\":\"\",\"password\":\"\",\"jdbcTable\":\"\",\"jdbcType\":\"\"}` select * from table\n");
        helpSb.append("\n");

        System.out.println(helpSb.toString());
    }
}

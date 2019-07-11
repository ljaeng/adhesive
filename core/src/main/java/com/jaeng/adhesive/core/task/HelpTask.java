package com.jaeng.adhesive.core.task;

import com.jaeng.adhesive.core.job.AbstractJob;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class HelpTask extends AbstractTask {

    private static String helpStr;

    @Override
    public void process(AbstractJob job, String line, SparkSession sparkSession, Map<String, Object> context) {
        super.process(job, line, sparkSession, context);

        System.out.println(getHelpStr());
    }

    private String getHelpStr() {
        if (StringUtils.isEmpty(helpStr)) {
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
            helpSb.append("\tcreate table table_name as text.`/path`;\n");
            helpSb.append("\tcreate table table_name as json.`/path`;\n");
            helpSb.append("\tcreate table table_name as parquet.`/path`;\n");
            helpSb.append("\tcreate table table_name as jdbc.`{\"url\":\"\", \"userName\":\"\",\"password\":\"\",\"jdbcTable\":\"\",\"jdbcType\":\"\"}`;\n");
            helpSb.append("\tcreate table table_name as select * from table1;\n");
            helpSb.append("\tcreate table table2 as select *,${a} from table1; 引用变量 a;\n");
            helpSb.append("\n");

            helpSb.append("数据输出:\n");
            helpSb.append("\tsave 类型.`{json配置}` | `路径`;\n");
            helpSb.append("\tsave json.`/path` select * from table_name; 保存到HDFS;\n");
            helpSb.append("\tsave text.`/path`select * from table_name; 保存到HDFS;\n");
            helpSb.append("\tsave parquet.`/path` select * from table_name; 保存到HDFS;\n");
            helpSb.append("\tsave mysql.`{\"url\":\"\", \"userName\":\"\",\"password\":\"\",\"jdbcTable\":\"\",\"jdbcType\":\"\"}` select * from table_name\n");
            helpSb.append("\n");

            helpSb.append("转换操作:\n");
            helpSb.append("\tprocess repatition.`{\"table\":\"table_name\",\"partiton\":1}`;\n");
            helpSb.append("\n");

            helpSb.append("缓存:\n");
            helpSb.append("\tset cache = true;表示下一个SQL数据会缓存\n");
            helpSb.append("\n");

            helpSb.append("清除缓存:\n");
            helpSb.append("\tclear cache;清除所有缓存\n");
            helpSb.append("\n");

            helpSb.append("查看表结构:\n");
            helpSb.append("\tshow table_name;\n");
            helpSb.append("\n");

            helpSb.append("查看表的数据:\n");
            helpSb.append("\tselect * from table_name;\n");
            helpSb.append("\n");

            helpSb.append("插件:\n");
            helpSb.append("\tprocess plugin.`{\"to\":\"\",\"msg\":\"\", \"condition\":\"2>1\"}`;\n");
            helpSb.append("\n");

            helpStr = helpSb.toString();
        }

        return helpStr;
    }
}

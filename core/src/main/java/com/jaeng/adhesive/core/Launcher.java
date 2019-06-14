package com.jaeng.adhesive.core;

import com.jaeng.adhesive.common.util.ParamParse;
import com.jaeng.adhesive.core.api.Job;
import com.jaeng.adhesive.core.job.FileJob;
import com.jaeng.adhesive.core.job.JobMode;
import com.jaeng.adhesive.core.job.JsonJob;
import com.jaeng.adhesive.core.job.LineJob;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class Launcher {

    /**
     * @param args -master master地址 -config 配置文件 -mode Job类型
     */
    public static void main(String[] args) {

        args = new String[]{"-master", "local", "-mode", "line"};

        ParamParse paramParse = ParamParse.parse(args);

        Job job = null;
        if (JobMode.JSON_MODE.getDesc().equals(paramParse.getMode())) {
            job = new JsonJob();
        } else if (JobMode.LINE_MODE.getDesc().equals(paramParse.getMode())) {
            job = new LineJob();
        }else if (JobMode.FILE_MODE.getDesc().equals(paramParse.getMode())) {
            job = new FileJob();
        }
        if (job != null) {
            job.init(paramParse);
            job.run();
            job.release();
        }
    }

}

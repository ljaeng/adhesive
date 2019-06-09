package com.jaeng.adhesive.launcher;

import com.jaeng.adhesive.common.util.ParamParse;
import com.jaeng.adhesive.core.component.Job;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class Launcher {

    /**
     * @param args -master master地址 -config 配置文件 -mode Job类型
     */
    public static void main(String[] args) {
        ParamParse paramParse = ParamParse.parse(args);

        Job job = null;
        if (JobMode.JSON_MODE.equals(paramParse.getMode())) {
            job = new JsonJob();
            job.init(paramParse.getConfig());
        } else if (JobMode.LINE_MODE.equals(paramParse.getMode())) {

        }
        if (job != null) {
            job.run();
            job.release();
        }
    }

}

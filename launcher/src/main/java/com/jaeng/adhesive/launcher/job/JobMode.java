package com.jaeng.adhesive.launcher.job;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public enum JobMode {

    JSON_MODE("json"),
    FILE_MODE("file"),
    LINE_MODE("line"),;

    String desc;

    JobMode(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }
}

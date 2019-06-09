package com.jaeng.adhesive.launcher;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public enum JobMode {

    JSON_MODE("json"),
    LINE_MODE("line"),;

    String desc;

    JobMode(String desc) {
        this.desc = desc;
    }
}

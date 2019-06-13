package com.jaeng.adhesive.common.enums;

/**
 * @author lizheng
 * @date 2019/6/10
 */
public enum ComponentEnum {

    INPUT("input", "Input", "com.jaeng.adhesive.input"),
    OUTPUT("output", "Output", "com.jaeng.adhesive.output"),
    PROCESS("process", "Process", "com.jaeng.adhesive.process"),
    PLUGIN("plugin", "Plugin", "com.jaeng.adhesive.plugin"),
    UDF("udf", "Udf", "com.jaeng.adhesive.udf"),

    ;

    String type;
    String upType;
    String packageName;

    ComponentEnum(String type, String upType, String packageName) {
        this.type = type;
        this.upType = upType;
        this.packageName = packageName;
    }

    public String getType() {
        return type;
    }

    public String getUpType() {
        return upType;
    }

    public String getPackageName() {
        return packageName;
    }
}

package com.jaeng.adhesive.common.enums;

/**
 * @author lizheng
 * @date 2019/6/10
 */
public enum ComponentTypeEnum {

    SOURCE("source", "Source", "com.jaeng.adhesive.core.source"),
    SINK("sink", "Sink", "com.jaeng.adhesive.core.sink"),
    PROCESS("process", "Process", "com.jaeng.adhesive.core.process"),
    PLUGIN("plugin", "Plugin", "com.jaeng.adhesive.core.plugin"),
    UDF("udf", "Udf", "com.jaeng.adhesive.core.udf"),

    ;

    String type;
    String upType;
    String packageName;

    ComponentTypeEnum(String type, String upType, String packageName) {
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

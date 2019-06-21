package com.jaeng.adhesive.common.util;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * 项目启动参数的解析工具
 *
 * @author lizheng
 * @date 2019/6/9
 */
public class ParamParse {

    private Map<String, Object> parseData;

    private ParamParse(Map<String, Object> parseData) {
        this.parseData = parseData;
    }

    public String getMaster() {
        return parseData.getOrDefault("master", "").toString();
    }

    public String getName() {
        return parseData.getOrDefault("name", "").toString();
    }

    public String getConfig() {
        return parseData.getOrDefault("config", "").toString();
    }

    public String getMode() {
        return parseData.getOrDefault("mode", "").toString();
    }

    public static ParamParse parse(String[] args) {
        Map<String, Object> parseData = new HashMap<>(CollectionUtil.initSize(args.length / 2, CollectionUtil.HASH_MAP_DEFAULT_LOAD_FACTOR));
        String key = null;
        for (String arg : args) {
            if (key != null) {
                parseData.put(key, arg);
                key = null;
            } else {
                key = arg.trim().substring(1);
            }
        }
        return new ParamParse(parseData);
    }

    public static void main(String[] args) {
        args = new String[]{"-master", "master地址", "-config ", "配置文件", "-mode"};
        System.out.println(JSONObject.toJSONString(ParamParse.parse(args)));
    }
}

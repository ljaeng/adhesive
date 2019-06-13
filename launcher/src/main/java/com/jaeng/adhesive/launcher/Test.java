package com.jaeng.adhesive.launcher;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.MapUtils;

import java.util.Set;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class Test {

    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        json.put("a", 1);
        json.put("c", false);

        System.out.println(MapUtils.getInteger(json, "a", 2));
        System.out.println(MapUtils.getInteger(json, "b", 2));
        System.out.println(MapUtils.getBooleanValue(json, "c", true));
        System.out.println(MapUtils.getBooleanValue(json, "d", false));

        System.out.println(Math.ceil(2.1));
        System.out.println(Math.ceil(12 / 0.75));
        System.out.println(Math.ceil(13 / 0.75));

    }

}
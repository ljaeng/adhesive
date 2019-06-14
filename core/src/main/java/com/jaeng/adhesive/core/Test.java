package com.jaeng.adhesive.core;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.util.PatternUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public class Test {

    public static void main(String[] args) {
        String line = "insert kafka.`{\"broker\":\"172.17.30.212:9092,172.17.30.213:9092,172.17.30.214:9092,172.17.30.215:9092,172.17.30.216:9092\",\"topic\":\"socrates.yzb_model_live\",\"jsonField\":\"value\"}`";


        String regex = "insert +(.*?) *. *`(.*)` *(.*)";
        String type = PatternUtil.getValueByRegex(regex, line, 1);
        String conf = PatternUtil.getValueByRegex(regex, line, 2);
        String sql = PatternUtil.getValueByRegex(regex, line, 3);

        System.out.println(type);
        System.out.println(conf);
        System.out.println(sql);
        System.out.println(StringUtils.isNotBlank(sql));
    }

}
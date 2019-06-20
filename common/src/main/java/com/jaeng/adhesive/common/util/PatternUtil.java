package com.jaeng.adhesive.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lizheng
 * @date 2019/6/13
 */
public class PatternUtil {

    public static boolean findValueByRegex(String regex, String str) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

    public static String getValueByRegex(String regex, String str, int location) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        matcher.find();
        return matcher.group(location);
    }

}

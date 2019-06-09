package com.jaeng.adhesive.common.util;

import java.util.List;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class NumberUtil {

    static public boolean isNum(List<Object> ids) {
        if (ids != null && ids.size() > 0) {
            Object obj = ids.get(0);
            if (obj != null) {
                if (obj instanceof Integer || obj instanceof Double || obj instanceof Float) {
                    return true;
                }
            }
        }
        return false;
    }

}

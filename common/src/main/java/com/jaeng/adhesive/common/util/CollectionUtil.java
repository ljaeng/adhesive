package com.jaeng.adhesive.common.util;

/**
 * @author lizheng
 * @date 2019/6/8
 */
public class CollectionUtil {

    public static final float HASH_MAP_DEFAULT_LOAD_FACTOR = 0.75f;

    public static int initSize(int size, float loadFactor) {
        if (size > 0) {
            return (int)Math.ceil(size / loadFactor);
        } else {
            return 0;
        }
    }

}

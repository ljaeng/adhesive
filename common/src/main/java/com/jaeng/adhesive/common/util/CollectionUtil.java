package com.jaeng.adhesive.common.util;

/**
 * 集合工具
 *
 * @author lizheng
 * @date 2019/6/8
 */
public class CollectionUtil {

    public static final float HASH_MAP_DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * 根据容量和负载因子获取合适的HashMap初始容量
     *
     * @param size
     * @param loadFactor
     * @return
     */
    public static int initSize(int size, float loadFactor) {
        if (size > 0) {
            return (int) Math.ceil(size / loadFactor);
        } else {
            return 0;
        }
    }

}

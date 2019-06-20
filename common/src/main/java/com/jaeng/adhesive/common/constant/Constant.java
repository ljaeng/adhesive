package com.jaeng.adhesive.common.constant;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * 静态常量
 *
 * @author lizheng
 * @date 2019/6/9
 */
public class Constant {

    public static final DateFormat SIMPLE_DATE_FORMAT_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    public static final DateFormat SIMPLE_DATE_FORMAT_YYYYMM01 = new SimpleDateFormat("yyyyMM01");

    public static final long ONE_DAT_TIME = 24 * 60 * 60 * 1000;

    public static final String CACHE_DATASET_LIST = "cache_dataset_list";

    public static final String BROADCAST_LIST = "broadcast_list";

}

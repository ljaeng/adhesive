package com.jaeng.adhesive.core.udf;

import java.util.Arrays;
import java.util.List;

/**
 * @author lizheng
 * @date 2019/7/11
 */
public class UDFConstant {

    public static final List<Class> UDF_LIST = Arrays.asList(
            DateFormatUdf.class,
            JoinFilePathWithTimeRangeUdf.class,
            JsonFieldUdf.class,
            TextSplitUdf.class
    );

}

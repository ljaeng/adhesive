package com.jaeng.adhesive.core.api;

import java.io.Serializable;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public interface Component extends Configurable, Runnable, Serializable {

    public static final String LAST_DATASET = "lase_dataset";

}

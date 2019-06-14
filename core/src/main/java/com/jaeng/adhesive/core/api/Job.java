package com.jaeng.adhesive.core.api;

import java.io.Serializable;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public interface Job extends Runnable, Serializable {

    void init(Object config);

    void run();

    void release();
}

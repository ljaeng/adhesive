package com.jaeng.adhesive.core;


import com.alibaba.fastjson.JSONObject;

/**
 * @author lizheng
 * @date 2019/6/6
 */
public interface Configurable {

    void setConf(JSONObject conf);

    JSONObject getConf();

}

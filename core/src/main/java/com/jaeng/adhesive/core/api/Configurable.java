package com.jaeng.adhesive.core.api;


import com.alibaba.fastjson.JSONObject;

/**
 * 支持配置
 *
 * @author lizheng
 * @date 2019/6/6
 */
public interface Configurable {

    /**
     * 设置配置
     *
     * @param conf
     */
    void setConf(JSONObject conf);

    /**
     * 获取配置
     *
     * @return
     */
    JSONObject getConf();

}

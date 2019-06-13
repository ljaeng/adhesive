package com.jaeng.adhesive.core.component;

import com.alibaba.fastjson.JSONObject;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public abstract class AbstractPlugin implements Plugin {

    protected JSONObject conf;

    @Override
    public void setConf(JSONObject conf) {
        this.conf = conf;
    }

    @Override
    public JSONObject getConf() {
        return this.conf;
    }


}

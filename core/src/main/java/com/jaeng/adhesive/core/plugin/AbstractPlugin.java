package com.jaeng.adhesive.core.plugin;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.core.api.Plugin;

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

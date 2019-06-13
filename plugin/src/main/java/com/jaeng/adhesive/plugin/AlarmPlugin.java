package com.jaeng.adhesive.plugin;

import com.alibaba.fastjson.JSONObject;
import com.jaeng.adhesive.common.constant.PluginConstant;
import com.jaeng.adhesive.common.util.BeetlTemplateUtil;
import com.jaeng.adhesive.common.util.HttpUtil;
import com.jaeng.adhesive.core.component.AbstractPlugin;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public class AlarmPlugin extends AbstractPlugin {

    private static final Logger logger = LoggerFactory.getLogger(AlarmPlugin.class);

    private String type;
    private String condition;
    private String msg;
    private String to;
    private String url;

    @Override
    public void setConf(JSONObject conf) {
        super.setConf(conf);

        this.type = MapUtils.getString(conf, "type", PluginConstant.PLUGIN_ALARM_TYPE_WEIBO);
        this.condition = MapUtils.getString(conf, "condition", null);
        this.msg = MapUtils.getString(conf, "msg", null);
        this.to = MapUtils.getString(conf, "to", null);


        if (PluginConstant.PLUGIN_ALARM_TYPE_WEIBO.equals(type)) {
            this.url = "http://172.17.31.2/alps/alert/weibo";
        } else if (PluginConstant.PLUGIN_ALARM_TYPE_WEIBO.equals(type)) {
            this.url = "http://172.17.31.2/alps/alert/dingding";
        }
    }

    @Override
    public void run(SparkSession sparkSession, Map<String, Object> context) {
        if (!StringUtils.isEmpty(condition) && !BeetlTemplateUtil.eval(context, condition)) {

            System.out.println("不符合条件，不报警!");
        } else {
            if (StringUtils.isEmpty(to)) {
                System.out.println("发送失败，发件人不能为空!");
            } else {

                if (StringUtils.isEmpty(msg)) {
                    msg = "内容为空!";
                }
                Map<String, String> requestData = new HashMap<>();
                requestData.put("msg", msg);
                requestData.put("to", to);
                logger.info("发送告警, params:{}", JSONObject.toJSONString(requestData));
                String result = HttpUtil.sendHttpGet(url, requestData);
                logger.info("发送微信告警, result:{}", result);
            }

        }
    }

}

package com.jaeng.adhesive.common.util;

import org.apache.commons.lang.StringUtils;
import org.beetl.core.*;
import org.beetl.core.resource.StringTemplateResourceLoader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Beetl模版工具
 *
 * @author lizheng
 * @date 2019/6/9
 */
public class BeetlTemplateUtil {

    private static GroupTemplate gt = null;

    static {
        init();
    }

    public static void init() {
        StringTemplateResourceLoader resourceLoader = new StringTemplateResourceLoader();
        Configuration cfg = null;
        try {
            cfg = Configuration.defaultConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }

        gt = new GroupTemplate(resourceLoader, cfg);
        gt.registerFunction("dateToMS", new Function() {
            @Override
            public Object call(Object[] paras, Context ctx) {

                String time = paras[0].toString();
                String pattern = paras[1].toString();

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                try {
                    return simpleDateFormat.parse(time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return 0;
                }
            }
        });
    }

    public static String render(Map<String, Object> param, String template) {
        if (param == null) {
            return template;
        } else {
            Template t = gt.getTemplate(template);

            for (Map.Entry<String, Object> p : param.entrySet()) {

                t.binding(p.getKey(), p.getValue());
            }
            return t.render();
        }


    }

    public static boolean eval(Map<String, Object> param, String expression) {

        if (!StringUtils.isEmpty(expression)) {
            String text = "<%\n" +
                    "var expression_eval = (" + expression + ");\n" +
                    "%>\n" +
                    "${expression_eval}";
            try {
                String result = render(param, text);
                if (!StringUtils.isEmpty(result) && "true".equals(result.trim())) {
                    return true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public static void main(String[] args) {

        Map<String, Object> data = new HashMap<>();
        data.put("key1", "v1");
        data.put("key2", "20161210");

        Map<String, Object> data2 = new HashMap<>();
        data2.put("id_1", "v-1-1");
        data.put("key_sub", data2);

        System.out.println(data);
        System.out.println(render(data, "<%\n" +
                "var a = {};\n" +
                "print(a);\n" +
                "\n%>\n"
                + "${key1}, ${a}, ${date(), 'yyyy-MM-dd'}, ${key_sub.id_1}"));
    }
}

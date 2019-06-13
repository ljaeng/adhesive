package com.jaeng.adhesive.core.component;

import com.jaeng.adhesive.common.enums.ComponentEnum;
import com.jaeng.adhesive.common.util.ClassUtil;
import com.jaeng.adhesive.common.util.ParamParse;
import com.jaeng.adhesive.core.Componentable;
import com.jaeng.adhesive.core.Registerable;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public abstract class AbstractJob implements Job, Componentable {

    protected ParamParse conf;

    protected SparkSession sparkSession;

    protected Map<String, Component> componentCache = new HashMap<>(20);

    private static final Logger logger = LoggerFactory.getLogger(ClassUtil.class);

    private void initSparkSession() {
        SparkSession.Builder builder = SparkSession.builder();

        if (StringUtils.isNotEmpty(conf.getMaster())) {
            builder.master(conf.getMaster());
        }

        this.sparkSession = builder.getOrCreate();

        UDFRegistration udfRegistration = sparkSession.sqlContext().udf();
        //注册Udf
        Set<Class<?>> udfClassSet = ClassUtil.getClassSet(ComponentEnum.UDF.getType());
        for (Class<?> udfClass : udfClassSet) {
            try {
                Registerable udf = (Registerable) udfClass.newInstance();
                udfRegistration.registerJava(udf.getRegisterName(), udfClass.getName(), udf.getDataType());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void init(Object config) {
        ParamParse conf = (ParamParse) config;
        this.conf = conf;

        this.initSparkSession();
    }

    @Override
    public Component getComponent(String type, String name) throws Exception {
        String packageName;
        String className = ClassUtil.classNameUp(name);
        if (ComponentEnum.INPUT.equals(type)) {
            packageName = ComponentEnum.INPUT.getPackageName();
        } else if (ComponentEnum.OUTPUT.equals(type)) {
            packageName = ComponentEnum.OUTPUT.getPackageName();
        } else if (ComponentEnum.PROCESS.equals(type)) {
            packageName = ComponentEnum.PROCESS.getPackageName();
        } else if (ComponentEnum.PLUGIN.equals(type)) {
            packageName = ComponentEnum.PLUGIN.getPackageName();
        } else {
            String message = String.format("未知的组件,type:%s,name:%s", type, name);
            logger.warn(message);
            throw new Exception(message);
        }
        String classPath = String.format("%s.%s", packageName, className);

        Component component;
        if (componentCache.containsKey(classPath)) {
            component = componentCache.get(classPath);
        } else {
            Class<?> aClass = ClassUtil.loadClass(classPath);
            component = ((Component) aClass.newInstance());
            componentCache.put(classPath, component);
        }
        return component;
    }

    @Override
    public void run() {
        Map<String, Object> context = new HashMap<>(50);
        run(this.sparkSession, context);
    }

    @Override
    public void release() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}

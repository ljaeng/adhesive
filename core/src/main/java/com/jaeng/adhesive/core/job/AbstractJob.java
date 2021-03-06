package com.jaeng.adhesive.core.job;

import com.jaeng.adhesive.common.constant.Constant;
import com.jaeng.adhesive.common.enums.ComponentTypeEnum;
import com.jaeng.adhesive.common.util.ClassUtil;
import com.jaeng.adhesive.common.util.ParamParse;
import com.jaeng.adhesive.core.api.Component;
import com.jaeng.adhesive.core.api.Componentable;
import com.jaeng.adhesive.core.api.Job;
import com.jaeng.adhesive.core.api.Registerable;
import com.jaeng.adhesive.core.udf.*;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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

        if (StringUtils.isNotBlank(conf.getMaster())) {
            builder.master(conf.getMaster());
        }

        if (StringUtils.isNotBlank(conf.getName())) {
            builder.appName(conf.getName());
        }

        this.sparkSession = builder.getOrCreate();

        UDFRegistration udfRegistration = sparkSession.sqlContext().udf();
        //注册Udf
        //TODO:反射方式本地生效，打了Jar之后会报错，需要排查原因
//        Set<Class<?>> udfClassSet = ClassUtil.getClassSet(ComponentTypeEnum.UDF.getPackageName());
//        for (Class<?> udfClass : udfClassSet) {
//            try {
//                Registerable udf = (Registerable) udfClass.newInstance();
//                udfRegistration.registerJava(udf.getRegisterName(), udfClass.getName(), udf.getDataType());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
        for (Class udfClass : UdfRegisterConstant.UDF_REGISTER_LIST) {
            try {
                AbstractUdf udf = (AbstractUdf) udfClass.newInstance();
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
        String classType = null;
        if (ComponentTypeEnum.SOURCE.getType().equals(type)) {
            packageName = ComponentTypeEnum.SOURCE.getPackageName();
            classType = ComponentTypeEnum.SOURCE.getUpType();
        } else if (ComponentTypeEnum.SINK.getType().equals(type)) {
            packageName = ComponentTypeEnum.SINK.getPackageName();
            classType = ComponentTypeEnum.SINK.getUpType();
        } else if (ComponentTypeEnum.PROCESS.getType().equals(type)) {
            packageName = ComponentTypeEnum.PROCESS.getPackageName();
            classType = ComponentTypeEnum.PROCESS.getUpType();
        } else if (ComponentTypeEnum.PLUGIN.getType().equals(type)) {
            packageName = ComponentTypeEnum.PLUGIN.getPackageName();
            classType = ComponentTypeEnum.PLUGIN.getUpType();
        } else {
            String message = String.format("未知的组件,type:%s,name:%s", type, name);
            logger.warn(message);
            System.out.println(message);
            throw new Exception(message);
        }

        String classPath = String.format("%s.%s%s", packageName, className, classType);
        Component component;
        if (componentCache.containsKey(classPath)) {
            component = componentCache.get(classPath);
        } else {
            try {
                Class<?> aClass = ClassUtil.loadClass(classPath);
                component = ((Component) aClass.newInstance());
                componentCache.put(classPath, component);
            } catch (Exception e) {
                String message = String.format("初始化组件失败, %s", classPath);
                System.out.println(message);
                logger.warn(message, e);
                throw new Exception(message);
            }
        }
        return component;
    }

    @Override
    public void run() {
        Map<String, Object> context = new HashMap<>(64);
        context.put(Constant.CACHE_DATASET_LIST, new LinkedList<Dataset>());
        context.put(Constant.BROADCAST_LIST, new HashMap<String, Broadcast>(16));

        context.putAll(conf.getArgs());

        run(this.sparkSession, context);
    }

    @Override
    public void release() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}

package com.jaeng.adhesive.core.udf;

import com.jaeng.adhesive.core.api.Registerable;

/**
 * @author lizheng
 * @date 2019/7/11
 */
public abstract class AbstractUdf implements Registerable {

    /**
     * Help描述
     *
     * @return
     */
    final public String desc() {

        StringBuilder descSb = new StringBuilder();

        descSb.append("方法名: ")
                .append(getRegisterName())
                .append(",  ")
                .append("使用说明 :")
                .append(use_desc());

        return descSb.toString();
    }
}

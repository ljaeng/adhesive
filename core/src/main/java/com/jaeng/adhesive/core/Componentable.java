package com.jaeng.adhesive.core;

import com.jaeng.adhesive.core.component.Component;

/**
 * @author lizheng
 * @date 2019/6/9
 */
public interface Componentable<T extends Component> {

    T getComponent(String type, String name);

}

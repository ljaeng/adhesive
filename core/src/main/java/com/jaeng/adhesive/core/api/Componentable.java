package com.jaeng.adhesive.core.api;

/**
 * 组件化
 *
 * @author lizheng
 * @date 2019/6/9
 */
public interface Componentable<T extends Component> {

    /**
     * 获取组件实例
     *
     * @param type
     * @param name
     * @return
     */
    T getComponent(String type, String name) throws Exception;

}

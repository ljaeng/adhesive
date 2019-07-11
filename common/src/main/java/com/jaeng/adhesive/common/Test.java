package com.jaeng.adhesive.common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author lizheng
 * @date 2019/7/4
 */
public class Test {

    public static void main(String[] args) {

        CallInterface c = new CallC();

        CallInterface cProxy = (CallInterface) Proxy.newProxyInstance(
                c.getClass().getClassLoader(),
                c.getClass().getInterfaces(),
                new MyInvocationHandler(c));

        System.out.println(cProxy.call());

    }

    interface CallInterface {
        String call();
    }

    static class CallC implements CallInterface {

        @Override
        public String call() {
            return "Call Method Invoke";
        }
    }


    static class MyInvocationHandler implements InvocationHandler {

        Object object;

        public MyInvocationHandler(Object object) {
            this.object = object;
        }

        private void befor() {
            System.out.println("===befor===");
        }

        private void after() {
            System.out.println("===after===");
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                befor();
                return method.invoke(this.object, args);
            } finally {
                after();
            }
        }
    }

}

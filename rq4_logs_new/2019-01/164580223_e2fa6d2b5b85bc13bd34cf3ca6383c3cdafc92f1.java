package com.ejb;

import javax.ejb.Stateless;

@Stateless(name = "HellWorldBeanEJB")
public class HellWorldBeanBean implements HelloWorld{
    public HellWorldBeanBean() {
    }

    @Override
    public String sayHello(String world) {
        return "Hello " + world + "!";
    }
}
package com.vipjoy.joy.api;

import weixin.popular.support.TokenManager;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;


@WebListener
public class InitController extends BaseController implements ServletContextListener{
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        //初始化调用
        TokenManager.init(APPID, APPSSECRET);
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        //销毁
        TokenManager.destroyed();
    }
}
package com.project.common;

import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import javax.servlet.annotation.WebListener;

@WebListener
@Slf4j
public class RequestListener implements ServletRequestListener {

    @Override
    public void requestDestroyed(ServletRequestEvent servletRequestEvent) {
        log.info("请求发起");
        BaseController.ActionInit();
    }

    @Override
    public void requestInitialized(ServletRequestEvent servletRequestEvent) {
        log.info("请求销毁");
    }
}
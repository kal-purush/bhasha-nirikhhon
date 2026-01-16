package com.example.servlets;

import com.example.controllers.ControllerSupportService;
import com.example.utils.beans.factory.BeanFactorySupportService;
import com.example.utils.beans.factory.stereotype.ComponentSupportService;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ComponentSupportService
public class DispatcherServletSupportService extends HttpServlet {
    private Map<String, Object> controllers = new ConcurrentHashMap<>();

    @Override
    public void init() throws ServletException {
        BeanFactorySupportService beanFactory = new BeanFactorySupportService();
        beanFactory.fillSingletonsMap("com.example");
        beanFactory.fillAutowired();
        Object helpController = beanFactory.getBeans("helpController");
        controllers.put("/help-service/v1/support", helpController);
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String uri = req.getRequestURI();
        String contextPath = req.getContextPath();
        String path = uri.substring(contextPath.length());

        ControllerSupportService controller = (ControllerSupportService) controllers.get(path);
        if (controller != null) {
            controller.handleRequest(req, resp);
        } else {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found");
        }
    }
}
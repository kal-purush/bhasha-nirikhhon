package com.project.interceptor;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class AuthInterceptor extends HandlerInterceptorAdapter {

    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler)
            throws Exception {
        HttpSession httpSession = request.getSession();

        String requestUrl = request.getRequestURL().toString();
        if(requestUrl.contains("/login")){
            return true;
        }
        Object obj = httpSession.getAttribute("login");
        if (obj == null){
            response.sendRedirect("/login");
            return false;
        }
        return super.preHandle(request, response, handler);

    }
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView)
        throws Exception{
        super.postHandle(request, response, handler, modelAndView);
    }
}
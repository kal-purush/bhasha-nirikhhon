package com.danger.filter;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebFilter("/*")
public class LoginFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        //判断用户是否登录
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        response.setContentType("text/html;charset=utf-8");
        //获取请求路径
        String uri = req.getRequestURI();
        if (uri.contains("index.html") || uri.contains("login")) {//正常登录
            chain.doFilter(request, response);
        } else {
            Object user = req.getSession().getAttribute("user");//判断是否以登录
            if (user != null) {
                //已登录
                chain.doFilter(request, response);
            } else {
                resp.getWriter().write("<font color='green' size='25'>用户未登录 5秒之后跳转到主页....</font>");
                resp.setHeader("Refresh", "5;URL=" + req.getContextPath() + "index.html");
            }
        }
    }


    @Override
    public void destroy() {

    }
}
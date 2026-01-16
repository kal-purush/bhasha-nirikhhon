package com.gxl.study.javaweb.filter;


import javax.servlet.*;
import java.io.IOException;

/**
 * @author weilai
 * @description
 * @since 2019/12/17
 */
public class HelloFilter implements Filter {

    public void init(FilterConfig filterConfig) throws ServletException {
        System.out.println("======Filter do init ");
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        System.out.println("======Filter do doFilter ");
        //放行
        chain.doFilter(request,response);
    }

    public void destroy() {
        System.out.println("======Filter do destroy ");
    }
}
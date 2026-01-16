package com.exigenservices.lectures.filters;

// Import required java libraries
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

// Implements Filter class
public class MyFilter implements Filter  {
    public void destroy() {}
    public void init(FilterConfig fConfig) throws ServletException {}

    /**
     * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
     */
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        Object sessionStarted = ((HttpServletRequest)request).getSession(true).getAttribute("sessionStarted");
        if(sessionStarted==null){
            ((HttpServletRequest)request).getSession().getServletContext().getRequestDispatcher("/index.jsp").forward(request, response);
            //request.    getServletContext().getRequestDispatcher("index.jsp").forward(request, response);
        }else{
            chain.doFilter(request, response);
        }
    }
}
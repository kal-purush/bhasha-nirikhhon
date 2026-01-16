package com.exigenservices.lectures.filter;

/**
 * Created by Дарья on 29.11.2016.
 */
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

public class MyFilter implements Filter {
    public void destroy() {
    }

    public void init(FilterConfig fConfig) throws ServletException {
    }

    /**
     * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
     */
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        Object sessionStarted = ((HttpServletRequest) request).getSession(true).getAttribute("sessionStarted");
        if (sessionStarted == null) {
            ((HttpServletRequest) request).getSession().getServletContext().getRequestDispatcher("/start.jsp").forward(request, response);
        } else {
            chain.doFilter(request, response);

        }
    }
}
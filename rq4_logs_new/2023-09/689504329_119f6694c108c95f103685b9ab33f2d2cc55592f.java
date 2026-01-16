package com.mega.common.filter;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpFilter;
import java.io.IOException;

@WebFilter(urlPatterns = {"*"})
public class CharacterEncodingFilter extends HttpFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        ServletContext context = request.getServletContext();
        String encoding = context.getInitParameter("encoding");
        request.setCharacterEncoding(encoding);

        chain.doFilter(request, response);
    }
}
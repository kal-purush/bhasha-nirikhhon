package cn.hysian.filter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Servlet Filter implementation class LoginFilter
 */
@WebFilter("/LoginFilter")
public class LoginFilter implements Filter {

    /**
     * Default constructor. 
     */
    public LoginFilter() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Filter#destroy()
	 */
	public void destroy() {
		// TODO Auto-generated method stub
	}

	/**
	 * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
	 */
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		HttpServletRequest request1 = (HttpServletRequest)request;  
        HttpServletResponse response1 = (HttpServletResponse)response;  
//        HttpSession session = request.getSession();
        Cookie[] cookies = null;
        Cookie c = null;
        cookies = request1.getCookies();
        HttpSession session = request1.getSession();
//      System.out.println(cookies);
        if(cookies.length > 1){
	        c = cookies[1];
        	if(c.getName().equals("id")){
        		chain.doFilter(request1, response1);
        	}else{
        		response1.setCharacterEncoding("utf-8");  
        		PrintWriter out = response1.getWriter();  
        		out.print("<script>alert('您还没有登录，请登录...'); window.location='login.html' </script>");
        	}
        }else{
    		response1.setCharacterEncoding("utf-8");  
    		PrintWriter out = response1.getWriter();  
    		out.print("<script>alert('您还没有登录，请登录...'); window.location='login.html' </script>");
    	}
	}

	/**
	 * @see Filter#init(FilterConfig)
	 */
	public void init(FilterConfig fConfig) throws ServletException {
		// TODO Auto-generated method stub
	}

}
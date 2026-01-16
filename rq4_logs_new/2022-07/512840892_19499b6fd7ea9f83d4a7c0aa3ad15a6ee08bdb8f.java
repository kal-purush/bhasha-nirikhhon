//package br.com.heroes.Heroes.configs;
//
//import java.io.IOException;
//
//import javax.servlet.Filter;
//import javax.servlet.FilterChain;
//import javax.servlet.ServletException;
//import javax.servlet.ServletRequest;
//import javax.servlet.ServletResponse;
//import javax.servlet.http.HttpServletResponse;
//
//public class FilterProject implements Filter{
//	  @Override
//	    public void doFilter(ServletRequest req, ServletResponse res,
//	            FilterChain chain) throws IOException, ServletException {
//	        HttpServletResponse response = (HttpServletResponse) res;
//	        response.setHeader("Access-Control-Allow-Origin", "http://localhost:4200");
//	        response.setHeader("Access-Control-Expose-Headers", "Content-Disposition");
//	        response.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,PUT,OPTIONS");
//	        response.setHeader("Access-Control-Allow-Headers", "*");
//	        response.setHeader("Access-Control-Max-Age", "86400");
//	        chain.doFilter(req, res);
//	    }
//	}

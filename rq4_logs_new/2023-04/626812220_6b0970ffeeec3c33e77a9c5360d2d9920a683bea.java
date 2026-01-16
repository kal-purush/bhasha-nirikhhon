package com.billi.filter;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpFilter;
import javax.servlet.http.HttpServletRequest;
/**
 * Servlet Filter implementation class EncodingFilter
 */
@WebFilter("*.do") //.do로 끝나는 모든 파일,  /* = 모든 파일
public class EncodingFilter extends HttpFilter implements Filter {
      
    public EncodingFilter() {
       
    }
	public void destroy() {
		
	}
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		//요청필터
		request.setCharacterEncoding("utf-8");
		
		//강제형변환
		//자식 = (자식) 부모 
		HttpServletRequest req = (HttpServletRequest)request;
	
		// 서블릿으로 요청과 응답이 간다 (막으면 필터가 수행되지 않음
		chain.doFilter(request, response); 
		
		//응답 필터
		//브라우저에 응답하러 가고 있는 중이다.
	}
	public void init(FilterConfig fConfig) throws ServletException {
	
	}
}
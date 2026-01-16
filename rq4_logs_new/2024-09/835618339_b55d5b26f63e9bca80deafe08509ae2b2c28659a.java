package com.kh.erp.interceptor;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

@Service
public class LoginInterceptor implements HandlerInterceptor{
	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		//목표 : 로그인 한 사람은 차단, 아닌 사람은 통과(+홈페이지 이동)
		
		HttpSession session = request.getSession();
		String createdUser =  (String)session.getAttribute("createdUser");
		boolean isLogin = createdUser ==null;
		if(isLogin) {//로그인 안한 사람이라면
			return true;//통과		
		}
		else {//로그인 한 사람이라면
			response.sendRedirect("/home");//홈페이지로 추방
			return false;//차단
		}
		
	}
}
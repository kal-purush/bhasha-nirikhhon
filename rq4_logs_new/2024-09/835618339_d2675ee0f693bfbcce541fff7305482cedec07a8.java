package com.kh.erp.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.kh.erp.interceptor.AdminInterceptor;
import com.kh.erp.interceptor.EmpInterceptor;


@Configuration
public class InterceptorConfiguration implements WebMvcConfigurer  {
	@Autowired
	private AdminInterceptor adminInterceptor;
	@Autowired
	private EmpInterceptor empInterceptor;
	
	@Override
	public void addInterceptors(InterceptorRegistry registry) {
		registry.addInterceptor(empInterceptor)
				.addPathPatterns(
							"/tb/*",//모든 tb 페이지
							"/home"
									)
				.excludePathPatterns(
							"/tb/login",//로그인페이지
							"/",//메인페이지
							"/tb/findPw",//비밀번호찾기
							"/tb/join",//가입
							"/tb/resetPw"//비밀번호찾기 링크
						);
		registry.addInterceptor(adminInterceptor)
				.addPathPatterns(
							"/admin/**"//모든 admin 페이지
						);
	}
	
}
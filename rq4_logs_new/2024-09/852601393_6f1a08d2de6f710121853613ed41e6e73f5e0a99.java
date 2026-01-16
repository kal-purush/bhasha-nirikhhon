package com.boot.Config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.boot.interceptor.UserInterceptor;

@Configuration
public class WebConfig implements WebMvcConfigurer {
	
 	@Autowired
    private UserInterceptor userInterceptor;
	  
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
//    	registry.addInterceptor(new UserInterceptor())
        registry.addInterceptor(userInterceptor)
        		.addPathPatterns("/**");  // 모든 URL에 대해 적용;
    }
    
  
}
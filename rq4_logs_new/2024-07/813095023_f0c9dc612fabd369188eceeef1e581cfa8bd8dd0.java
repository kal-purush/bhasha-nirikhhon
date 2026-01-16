package com.spring.mood.projectmvc.config;

import com.spring.mood.projectmvc.interceptor.MyPageInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class myPageWebConfig implements WebMvcConfigurer {

    @Autowired
    private MyPageInterceptor interceptor;


    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 인터셉터를 등록하고 /mypage/** 경로에 대해 적용
        registry.addInterceptor(interceptor)
                .addPathPatterns("/mypage/**");
    }

}
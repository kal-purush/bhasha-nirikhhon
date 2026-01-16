package com.example.mealme.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Component
public class WebConfig implements WebMvcConfigurer {
    @Value("${file.dir}")
    private String fileDir;


    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry){
        // addResourceHandlers() 리소스 경로와 연결될 URL 경로를 작성한다.
        // 리소스는 자원이고 현재 우리가 필요한 자원은 이미지 경로이다.
        registry.addResourceHandler("/upload/**")
        // 실제 리소스가 존제하는 외부 경로를 알려준다.
                .addResourceLocations("file:" + fileDir);
        // 로컬 디스크 경로는 file: 을 반드시 사용해야 한다.
    }
}
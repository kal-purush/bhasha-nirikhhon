package com.example.endpoint.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

@Configuration
public class WebMvcConfig extends WebMvcConfigurationSupport {


    @Override
    protected void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.favorPathExtension(true) //是否支持后缀的方式
                .parameterName("mediaType")
                .defaultContentType(MediaType.APPLICATION_JSON)
                .mediaType("xml", MediaType.APPLICATION_XML)   //当后缀名称为xml的时候返回xml数据
                .mediaType("html", MediaType.TEXT_HTML)        //当后缀名称是html时候返回html页面
                .mediaType("json", MediaType.APPLICATION_JSON);//当后缀名称是json的时候返回json数据
    }

}
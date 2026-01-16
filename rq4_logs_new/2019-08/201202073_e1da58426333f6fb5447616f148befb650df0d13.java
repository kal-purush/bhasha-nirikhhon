package com.miyako.subject.service.redis.provider;

import com.alibaba.dubbo.config.spring.context.annotation.EnableDubboConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;


/**
 * ClassName SubjectServiceRedisProviderApplication
 * Description //TODO
 * Author weila
 * Date 2019-08-08-0008 15:29
 */
@ImportResource(value = {"classpath:provider.xml"})
@EnableDubboConfig
@SpringBootApplication(scanBasePackages = "com.miyako.subject")
public class SubjectServiceRedisProviderApplication{

    private static Logger logger = LoggerFactory.getLogger(SubjectServiceRedisProviderApplication.class);

    public static void main(String[] args){
        SpringApplication.run(SubjectServiceRedisProviderApplication.class, args);
        logger.info("===>:SubjectServiceRedisProviderApplication start...");
    }
}
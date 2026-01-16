package com.weiyi.springcloud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * @ClassName ConfigCenterMain3377
 * @Description:
 * @Author wangwy
 * @Date 2020/6/28 15:23
 * @Version 1.0
 */
@SpringBootApplication
@EnableDiscoveryClient
public class ConfigClientMain3377 {
    public static void main (String[] args) {
        SpringApplication.run(ConfigClientMain3377.class, args);
    }
}
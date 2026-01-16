package com.going;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication

@ImportResource(value = {"applicationContext-root.xml"})
//@EnableDiscoveryClient
//@EnableFeignClients
//@EnableConfigurationProperties({InterfaceUrlConfig.class, InnerApiUrlConfig.class, ServiceCode.class, ShopContinuousCodeGenerateConfig.class, InterceptGroupBean.class, IvrSenderConfig.class})
public class RedreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedreamApplication.class, args);
    }
}
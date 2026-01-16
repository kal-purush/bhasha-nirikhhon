/**
 * 
 */
package com.yan.springcloudstudy.testregisterserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * 
 * @Title: RegisterServer.java
 * @Package com.yan.springcloudstudy.testregisterserver
 * @Description: 服务注册中心
 * @author yan
 * @date 2017年8月31日
 * @version V1.0
 */
@EnableEurekaServer
@SpringBootApplication
public class RegisterServer {
    public static void main( String[] args )
    {
    	SpringApplication.run(RegisterServer.class, args);
    }
}
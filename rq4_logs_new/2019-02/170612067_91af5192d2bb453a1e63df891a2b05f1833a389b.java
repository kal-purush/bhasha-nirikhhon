package com.princeli.micro.services.spring.cloud.config.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

/**
 * @program: microservices-project
 * @description: ${description}
 * @author: ly
 * @create: 2019-02-27 14:00
 **/
@SpringBootApplication
@EnableConfigServer
public class SpringCloudConfigServer {

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudConfigServer.class,args);
    }
}
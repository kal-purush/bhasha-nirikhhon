package com.domeastudio.mappingo.servers.microservice.postgresql;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@EnableJpaRepositories(basePackages = {"com.domeastudio.mappingo.servers.microservice.h2.repository"})
@EntityScan(basePackages = {"com.domeastudio.mappingo.servers.microservice.h2.pojo"})
@SpringBootApplication
public class PostgreSqlApplication {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(PostgreSqlApplication.class, args);
    }
}
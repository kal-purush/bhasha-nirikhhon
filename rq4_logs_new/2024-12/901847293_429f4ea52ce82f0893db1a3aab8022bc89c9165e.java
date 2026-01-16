package com.tinqin.library.reporting.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;


@SpringBootApplication(scanBasePackages = "com.tinqin.library.reporting")
@EntityScan(basePackages = "com.tinqin.library.reporting.persistence.models")
@EnableJpaRepositories(basePackages = "com.tinqin.library.reporting.persistence.repositories")
@EnableFeignClients

public class ReportingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReportingApplication.class, args);
    }

}
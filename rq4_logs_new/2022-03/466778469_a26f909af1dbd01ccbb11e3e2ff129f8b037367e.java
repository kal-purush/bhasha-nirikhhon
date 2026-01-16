package com.cognizant.accountservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

import com.cognizant.accountservice.controller.AccountController;

import springfox.documentation.swagger2.annotations.EnableSwagger2;



@SpringBootApplication
@EnableFeignClients
@EnableSwagger2
public class AccountserviceApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AccountController.class);
	/**
	 * @param args -> Main Method
	 */
	public static void main(String[] args) { 
		SpringApplication.run(AccountserviceApplication.class, args);
		LOGGER.info("Account Micoservice");
	}
}
package com.info;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**@author vijay
 * Start of Spring Boot Application
 * Deploys and runs the application on Tomcat port : 9090 , configured in application.properties
 * 
 *
 */


@SpringBootApplication
public class InvestmentPortfolioApp {

	public static void main(String[] args) {
		SpringApplication.run(InvestmentPortfolioApp.class, args);
	}

}
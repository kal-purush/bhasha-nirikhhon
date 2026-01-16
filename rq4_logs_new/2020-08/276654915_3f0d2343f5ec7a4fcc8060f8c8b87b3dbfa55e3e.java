package com.capgemini.paas.users_service;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.WebApplicationInitializer;

import com.capgemini.paas.services.commonutility.ServiceProperties;

@Configuration
@EnableAspectJAutoProxy
@SpringBootApplication(scanBasePackages = {"com.capgemini.paas.users_service"})
@EnableJpaRepositories("com.capgemini.paas.users_service")
@EntityScan("com.capgemini.paas.users_service")
@ComponentScan("com.capgemini")
public class UserApplication extends SpringBootServletInitializer implements WebApplicationInitializer {

	private static Class<UserApplication> applicationClass = UserApplication.class;
	
	public static void main(String[] args) {
		SpringApplication.run(applicationClass, args);
	}
	
	@Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(applicationClass);
    }
	
	@Bean
	CommandLineRunner init() {
		return args -> {
			
			// Add any environment variables that are required in the application here
			// e.g. ServiceProperties.addEnvironmentProperty("USERNAME");
			// ServiceProperties can be referenced in a static context to retrieve properties
			// e.g. ServiceProperties.getEnvironmentProperty("USERNAME");
			
			// containsNullValues throws an exception if NULL values are found for any environment
			// variables added to the HashMap above - This is used to verify the variables before 
			// the service starts and then subsequently fails because they are missing
			ServiceProperties.containsNullValues();
			
		};
	}
	
}
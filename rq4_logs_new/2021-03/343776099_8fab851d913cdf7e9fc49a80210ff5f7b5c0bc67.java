package org.communication.retry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages = {"org.communication.retry"})
@EnableJpaRepositories(basePackages = {"org.communication.retry.repositories"})
@EnableTransactionManagement
@EntityScan(basePackages = {"org.communication.retry.entities"})
public class CommunicationRetryApplication {

	public static void main(String[] args) {
		SpringApplication.run(CommunicationRetryApplication.class, args);
	}

}
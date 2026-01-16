package com.seouldata.fest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableFeignClients(basePackages = {"com.seouldata.fest.domain.fest.client"})
@SpringBootApplication
@Import({com.seouldata.common.config.JpaConfig.class})
public class FestApplication {

	public static void main(String[] args) {
		SpringApplication.run(FestApplication.class, args);
	}

}
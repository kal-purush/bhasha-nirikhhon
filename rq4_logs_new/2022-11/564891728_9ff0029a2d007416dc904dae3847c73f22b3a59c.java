package com.example.ignitedropcreatepopulate;

import com.example.ignitedropcreatepopulate.properties.IgniteProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties
@EnableScheduling
public class IgnitedropcreatepopulateApplication  {
	public static void main(String[] args) {
		SpringApplication.run(IgnitedropcreatepopulateApplication.class, args);
	}
}
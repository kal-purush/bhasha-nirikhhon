package com.mkshkmrgpt.directflightconnectionservice;

import com.mkshkmrgpt.directflightconnectionservice.info.DirectFlightConnection;
import com.mkshkmrgpt.directflightconnectionservice.info.DirectFlightConnectionRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.sleuth.Sampler;
import org.springframework.cloud.sleuth.sampler.AlwaysSampler;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

@EnableEurekaClient
@EnableMongoAuditing
@SpringBootApplication
public class DirectFlightConnectionServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(DirectFlightConnectionServiceApplication.class, args);
	}

	@Bean
	CommandLineRunner createDirectFlightConnections(DirectFlightConnectionRepository directFlightConnectionRepository){
		return arg ->{
			directFlightConnectionRepository.deleteAll();
			directFlightConnectionRepository.save(Arrays.asList(
				new DirectFlightConnection("AirIndia","Mumbai","Delhi", Instant.now(),
						Instant.now().plus(8,ChronoUnit.HOURS) ),
				new DirectFlightConnection("Indigo","Mumbai","Pune", Instant.now(),Instant.now().plus(10,ChronoUnit.HOURS))
			));
		};
	}

	@Bean
    Sampler defaultSampler(){
	    return new AlwaysSampler();
    }
}
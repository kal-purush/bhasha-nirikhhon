package com.ac.common.fabric;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import lombok.Getter;


@Configuration
@PropertySource(value = "classpath:application-hospital.properties")
@Getter
public class HospitalInfoConfig {

	@Value("${hospital.name}")
	private String name;

	@Value("${hospital.domname}")
	private String domname;

	@Value("${hospital.ca_location}")
	private String ca_location;

	@Value("${hospital.peer_locations}")
	private String peerLocations;

	@Value("${hospital.orderer_locations}")
	private String ordererLocations;

	@Value("${hospital.eventhub_locations}")
	private String eventhubLocations;
	
}
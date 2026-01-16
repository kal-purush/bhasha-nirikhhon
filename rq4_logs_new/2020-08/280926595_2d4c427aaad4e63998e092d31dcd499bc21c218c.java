package com.ksm.wstbackoffice.config;

import com.ksm.wstbackoffice.controller.AddressControllerTest;
import com.ksm.wstbackoffice.controller.CountryControllerTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigBean {
    @Bean
	public CountryControllerTest countryControllerTest() {
		return new CountryControllerTest();
	}

	@Bean
	public AddressControllerTest addressControllerTest() {
		return new AddressControllerTest();
	}
}
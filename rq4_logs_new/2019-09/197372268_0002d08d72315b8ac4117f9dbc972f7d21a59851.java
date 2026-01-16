package com.metacube.training.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.metacube.training.service.MailSender;
import com.metacube.training.service.impl.MockMailSender;
import com.metacube.training.service.impl.SmtpMailSender;

/**
 *
 * This is the configuration class
 *
 */

@Configuration
public class AppConfig {
	
	
	
	
	@Bean
	@Primary
	public MailSender smtpMailSender() {
		return new SmtpMailSender();
	}
	
	
	@Bean
   	public MailSender mockMailSender() {
		return new MockMailSender();
	}
}
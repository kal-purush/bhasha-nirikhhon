package com.smyr.showmeyourrecipe.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

@Configuration
public class EmailConfig {
	@Bean
	public JavaMailSender javaMailSender() {
		JavaMailSenderImpl javaMailSenderImpl = new JavaMailSenderImpl();
		javaMailSenderImpl.setHost( "smtp.gmail.com" );
		javaMailSenderImpl.setPort( 587 );

		javaMailSenderImpl.setUsername( "showmeyourrecipe25@gmail.com" );
		javaMailSenderImpl.setPassword( "jgau izph birc dqcp" );

		Properties props = javaMailSenderImpl.getJavaMailProperties();
		props.put( "mail.transport.protocol", "smtp" );
		props.put( "mail.smtp.auth", "true" );
		props.put( "mail.smtp.starttls.enable", "true" );
		props.put( "mail.debug", "true" );

		return javaMailSenderImpl;
	}
}
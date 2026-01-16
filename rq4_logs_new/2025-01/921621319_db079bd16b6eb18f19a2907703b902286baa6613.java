package com.metamong.mt.global.config;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

@Configuration
@Profile("prod")
public class MailConfig {
    private static final String MAIL_SMTP_AUTH = "mail.smtp.auth";
    private static final String MAIL_DEBUG = "mail.smtp.debug";
    private static final String MAIL_CONNECTION_TIMEOUT = "mail.smtp.connectiontimeout";
    private static final String MAIL_SMTP_STARTTLS_ENABLE = "mail.smtp.starttls.enable";

    @Value("${mail.smtp.connection-timeout}")
    private int connectionTimeout;
    
    @Value("${mail.smtp.timeout}")
    private int timeout;
    
    @Value("${mail.smtp.write-timeout}")
    private int writeTimeout;
    
    @Value("${mail.starttls.enable}")
    private boolean starttls;
    
    @Value("${mail.smtp.auth}")
    private boolean auth;
    
    @Value("${mail.smtp.debug}")
    private boolean debug;
    
    @Value("${mail.host}")
    private String host;
    
    @Value("${mail.username}")
    private String username;
    
    @Value("${mail.password}")
    private String password;
    
    @Value("${mail.port}")
    private int port;
    
    @Bean
    public JavaMailSender javaMailSender() {
        JavaMailSenderImpl javaMailSender = new JavaMailSenderImpl();
        javaMailSender.setHost(host);
        javaMailSender.setUsername(username);
        javaMailSender.setPassword(password);
        javaMailSender.setPort(port);
        
        Properties properties = javaMailSender.getJavaMailProperties();
        properties.put(MAIL_SMTP_AUTH, auth);
        properties.put(MAIL_DEBUG, debug);
        properties.put(MAIL_CONNECTION_TIMEOUT, connectionTimeout);
        properties.put(MAIL_SMTP_STARTTLS_ENABLE, starttls);
        
        javaMailSender.setJavaMailProperties(properties);
        javaMailSender.setDefaultEncoding(StandardCharsets.UTF_8.toString());
        
        return javaMailSender;
    }
}
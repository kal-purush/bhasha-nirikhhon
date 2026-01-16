package com.javalearning.springboot.helloworld.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.javalearning.springboot.helloworld.utils.Address;
import com.javalearning.springboot.helloworld.utils.Student;

@Configuration
public class AppConfig {

	@Bean(name="addressBean") // method anme is the bean name
    public Address address() {
    	return new Address();
    }
	
	@Bean(name={"studentBean", "studentBean1", "studentComponent"},
	initMethod = "init", destroyMethod = "destroy")
	public Student student() {
		return new Student(address());
	}
}
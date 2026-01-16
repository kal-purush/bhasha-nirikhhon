package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.demo.dao.Interviewer_Repository;
import com.example.demo.entities.Interviewer;



@SpringBootApplication
public class HireApplication {

	public static void main(String[] args) {
		ApplicationContext context=SpringApplication.run(HireApplication.class, args);
		Interviewer_Repository i1 =context.getBean(Interviewer_Repository.class);
		Interviewer i2=new Interviewer();
		i2.setEmailid("ck@gmail.com");
		i2.setNo_Of_Interviews(0);
		i2.setPrimary_Skill("java");
		i2.setProfile_Image("hi");
		i2.setRound_Alloted("R2");
		i2.setSecondary_Skill("cpp");
		i2.setTertiary_Skill("python");
		i2.setUsername("Chinmayee");
		i1.save(i2);
		
		
	
		
		
	}

}
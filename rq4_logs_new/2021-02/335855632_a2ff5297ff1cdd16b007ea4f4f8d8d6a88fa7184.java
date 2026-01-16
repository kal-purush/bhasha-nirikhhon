package com.dynamicdudes.helper;

import java.time.LocalDate;
import java.time.Period;

import org.springframework.stereotype.Component;

@Component
public class AgeGenerator implements AgeGeneration {
	


		  public  int getAge(String date) 
		    { 
			  	int age;
		        
			  	
			  	LocalDate today = LocalDate.now();
			  
			  	
		        LocalDate dob
		            = LocalDate.parse(date); 
		        
		        Period p = Period.between(dob, today);
		        
		        age=p.getYears();
		        return age;
		        
		        	       
		    }
	}

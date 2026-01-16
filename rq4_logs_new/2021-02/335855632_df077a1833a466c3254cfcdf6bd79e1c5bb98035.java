package com.dynamicdudes.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.dynamicdudes.helper.AgeGeneration;
import com.dynamicdudes.helper.RandomScoreGenerator;
import com.dynamicdudes.model.LoanApplicant;


@Component
public class LoanFrontEndValidatorImpl implements LoanFrontEndValidator {
	
	@Autowired
	RandomScoreGenerator randomScoreGenerator;
	
	@Autowired 
	AgeGeneration ageGenerator;

    public void experience(LoanApplicant loanApplicant) {
		
		if(loanApplicant.getYears()==0 && loanApplicant.getMonths()<6)
		{
			loanApplicant.setStatus("Declined");
			loanApplicant.setDeclineReason("Experience less than 6 months! Sorry! Try again later");
		}
		if(loanApplicant.getStatus()!="Declined")
		{
			loanApplicant.setStatus();
		}

	}

	public void checkSalary(LoanApplicant loanApplicant) {
		
		if(loanApplicant.getAnnualSal()<10000)
		{
			loanApplicant.setStatus("Declined");
			loanApplicant.setDeclineReason("Salary less than minimum limit! Sorry!");
		}
		if(loanApplicant.getStatus()!="Declined")
		{
			loanApplicant.setStatus();
		}
		
	}

	public void generateScore(LoanApplicant loanApplicant) {
		int randomscore=randomScoreGenerator.getRandomScore();
		System.out.println("RandomScore="+randomscore);
		if(randomscore<400)
		{
			loanApplicant.setStatus("Declined");
			loanApplicant.setDeclineReason("Score less than minimum limit! Sorry!");
		}
		if(loanApplicant.getStatus()!="Declined")
		{
			loanApplicant.setStatus();
		}
		loanApplicant.setScore(randomscore);
		
	}

	public void age(LoanApplicant loanApplicant) {
		int age=ageGenerator.getAge(loanApplicant.getDob());
		System.out.println("Printing age"+age);
		if(age<18 || age>65)
		{
			loanApplicant.setStatus("Declined");
			loanApplicant.setDeclineReason("Not in the age limits! Sorry!");
		}
		if(loanApplicant.getStatus()!="Declined")
		{
			loanApplicant.setStatus();
		}
		
	}


}
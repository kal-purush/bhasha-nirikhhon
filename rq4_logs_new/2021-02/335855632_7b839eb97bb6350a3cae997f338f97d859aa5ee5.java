package com.dynamicdudes.customvalidation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.springframework.beans.factory.annotation.Autowired;

import com.dynamicdudes.service.LoanApplicantService;
import com.dynamicdudes.service.LoanApplicantServiceImpl;


public class CustomSsnConstraintValidator implements ConstraintValidator<CustomSsn,Long> {
	@Autowired
	private LoanApplicantService loanApplicantService;
	

	@Override
	public boolean isValid(Long ssn, ConstraintValidatorContext arg1) {
		
		//LoanApplicantService loanApplicantService = new LoanApplicantServiceImpl();
		
		if(loanApplicantService != null)
		{
			
			if( loanApplicantService.isUniqueSSN(ssn) == false)
			{
				
				return false;
			}
		}
		
		
		return true;
	}
	

}
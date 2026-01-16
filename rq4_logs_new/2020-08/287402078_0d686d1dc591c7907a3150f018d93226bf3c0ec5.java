package com.minesweeper.api.domain.validators;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.minesweeper.api.domain.LockerRequest;

public class LockerConstrainValidator implements ConstraintValidator<ValidLockerRequest, LockerRequest> {

	
	public void initialize(ValidLockerRequest constraint) {
	
	}
	
	@Override
	public boolean isValid(LockerRequest value, ConstraintValidatorContext context) {
		if (value.isExposed()) {
			return !value.isQuestion() && !value.isFlag() && !value.isUncheck();	
		} else if (value.isFlag()) {
			return !value.isQuestion() && !value.isExposed() && !value.isUncheck();
		} else if (value.isQuestion()) {
			return !value.isExposed() && !value.isFlag() && !value.isUncheck();
		} else if (value.isUncheck()) {
			return !value.isQuestion() && !value.isFlag() && !value.isExposed();
		}
		return false;
	}

}
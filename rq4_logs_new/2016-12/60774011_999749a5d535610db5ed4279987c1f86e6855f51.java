package gov.ca.emsa.service.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class RandomFailingErrorException extends Throwable{
	
	public RandomFailingErrorException(){}

}
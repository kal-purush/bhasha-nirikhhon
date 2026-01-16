package com.bridgelabz.userregistration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserInputValidation {
    // Creating isValidFirstName method which will return boolean 
    // to validate the first name given by user using regex
	
	public boolean isValidFirstName(String firstName) {
	    // regex pattern to check the firstName
        String firstNameRegex = "[A-Z]{1}[a-z]{2,}";  
	    Pattern pattern = Pattern.compile(firstNameRegex);
	    Matcher matcher = pattern.matcher(firstName);
	    return matcher.matches();
	}
}
package com.bridgelabz.userregistration;
import java.util.ArrayList;

public class EmailPattern {
    public static void main(String[] args) {
		// 1.Creating an Array list to store the email Addresses which need to be validated
        ArrayList<String> emailAddressList = new ArrayList<>();
        // 2.Adding the email addresses to list which need to be validated
        emailAddressList.add("abc@bridgelabz.co.in");
        emailAddressList.add("abc@yahoo.com");
        emailAddressList.add("abc-100@yahoo.com");
        emailAddressList.add("abc.100@yahoo.com");
        emailAddressList.add("abc111@abc.com");
        emailAddressList.add("abc-100@abc.net");
        emailAddressList.add("abc.100@abc.com.au");
        emailAddressList.add("abc@1.com");
        emailAddressList.add("abc@gmail.com.com");
        emailAddressList.add("abc.xyz@bridgelabz.co.in");
        emailAddressList.add("abc+100@gmail.com");
        emailAddressList.add("abc.xyz@bridgelabz.co");
        emailAddressList.add("abc-xyz@bridgelabz.co.in");
        emailAddressList.add("abc_bridgelabz.company56");
        emailAddressList.add("ab33c@_bridgel4abz*gooogle");
        emailAddressList.add("abc");
        emailAddressList.add("abc@.com.my");
        emailAddressList.add("abc123@gmail.a");
        emailAddressList.add("abc123@.com");
        emailAddressList.add("abc123@.com.com");
        emailAddressList.add(".abc@abc.com");
        emailAddressList.add("abc()*@gmail.com");
        emailAddressList.add("abc@%*.com");
        emailAddressList.add("abc..2002@gmail.com");
        emailAddressList.add("abc.@gmail.com");
        emailAddressList.add("abc@abc@gmail..com");
        emailAddressList.add("abc@gmail.com.1a");
        emailAddressList.add("ab33c@_bridgel4abz*gooogle");

        EmailPatternValidation emailPatternValidation = new EmailPatternValidation();
        // 4.Iterating and printing valid message if the email addresses entered are valid
        for (String emailAddress : emailAddressList) {
            if (emailPatternValidation.isValidEmail(emailAddress))
                System.out.println(emailAddress + " - Valid");
            else
                System.out.println(emailAddress + " - Not valid");
        }
    }
}
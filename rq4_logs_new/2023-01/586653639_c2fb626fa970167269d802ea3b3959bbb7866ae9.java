package com.upskill.java_2;

public class IfElseStatement2 {

	public static void main(String[] args) {
		  
		int age = 101; 
		
		if(age <=13){ 
			System.out.println("You Are Childen");
		}else if(age >13 && age <18){ 
			System.out.println("You Are Teenager");
		}else if(age>=60){ 
			if(age <100){ 
				System.out.println("You Are Senior");
			} else 
				System.out.println("You Are Champiom");
		}else{ 
			System.out.println("You Are Adulit");
		}

	}

}
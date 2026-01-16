package edu.handong.csee.java.practicewithjc;
import java.util.Scanner;
public class ChemistrySymbolExplainer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String element;
		
		System.out.println("Insert the element symbol: ");
		
		Scanner keyboard = new Scanner(System.in);
		element = keyboard.next(); 
		
		System.out.println("This element " + element +".");// print out the value of our variable
		
		if (element.equals("O") ) {
			System.out.println("Oxygen");
		
		}
		if (element.equals("H")) {
			System.out.println("Hydrogen");
		}
		if (element.equals("F")) {
			System.out.println("Flourine");
			
		}

	}

}
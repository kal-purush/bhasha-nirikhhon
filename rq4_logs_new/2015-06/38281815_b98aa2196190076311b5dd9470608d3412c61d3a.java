// Copyright The League of Amazing Programmers 2014
import javax.swing.JOptionPane;

/*
 * I have a pocket full of change. I hate doing math. Make me a program that
 * will calculate how much money I have without me having to use my brain. Then
 * make me a sandwich.
 */
public class ChangeCalculator {

	public static void main(String[] args) {

		String NumberOfNickles = JOptionPane.showInputDialog("How many nickles do you have?");

	
		int NumNickles = Integer.parseInt(NumberOfNickles);

	
		String NumberOfDimes = JOptionPane.showInputDialog("How many dimes do you have?");

		
		int NumDimes = Integer.parseInt(NumberOfDimes);

		// Ask the user how many quarters they have, and convert their answer
		String NumberOfquarters = JOptionPane.showInputDialog("How many quarters do you have?");

		
		int Numquarters = Integer.parseInt(NumberOfquarters);

		// Calculate how much money the user has and save it in a double variable 
		int Sum = NumNickles * 5 + NumDimes * 10 + Numquarters * 25;
		
		// Tell the user how much money they have
JOptionPane.showMessageDialog(null, "You have "+ Sum + " cents");
	}
}
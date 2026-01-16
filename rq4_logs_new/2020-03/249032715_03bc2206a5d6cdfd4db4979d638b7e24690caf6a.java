package _04_test_scores;

import javax.swing.JOptionPane;

public class testScores {
public static void main(String[] args) {
	
	
	
	
	
String answer = JOptionPane.showInputDialog("What is your test score");
	
double number = Double.parseDouble(answer);
	
	if(number>90) {
		JOptionPane.showMessageDialog(null,"WOW! Amazing Job!");
	}
	if(number>80 && number <90) {
		JOptionPane.showMessageDialog(null,"You did good");
	}
	if(number<80) {
		JOptionPane.showMessageDialog(null,"You need to study harder next time");
	}
}
}
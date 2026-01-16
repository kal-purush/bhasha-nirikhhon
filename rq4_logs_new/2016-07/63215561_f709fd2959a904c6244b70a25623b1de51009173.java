package Assignment1;

import java.util.Scanner;

public class Pattern1 {
	static int totalCharactersInARow = 0;   //stores the total number of characters in a row 
	static int totalRowsInPattern = 0;      //total no of rows in the whole pattern
	static int currentRow = 0;

	public static void main(String[] args) {
		
		//scan the user input
		int rows;
		System.out.println("Enter the no of rows");
		Scanner scanner = new Scanner(System.in);
		rows = scanner.nextInt();
		
		//determine the total characters and the total rows in the pattern
		totalCharactersInARow = (2 * rows) - 1;
		totalRowsInPattern = (2 * rows) - 1;
		
		//call method to print the pyramid 
		new Pattern1().wholePyramid(rows);
		
		//close the resources
		scanner.close();

	}

	/*prints the whole pyramid based on the number of rows */
	public void wholePyramid(int rows) {
		int loopVariable = 0;
		String output = "";
		Pattern1 modular = new Pattern1();
		for (; loopVariable < 2 * rows; loopVariable++) {
			output += modular.spaces(rows, loopVariable);  //call to space calculating method
			output += modular.numbers(rows, loopVariable);//call to numbers calculating method
		}

		System.out.println("Pattern:\n" + output);
	}

	/*calculates the number's string in a particular row */
	public String numbers(int rows, int n) {
		String output = "";
		int rowNumber = n;
		int temp = 1;
		int startingNumber = 1;
		temp = startingNumber;
		int middleNumber = totalRowsInPattern - n;
		
		//the if part is for the upper half of the pyramid
		if (n < rows) {
			while (temp <= rowNumber + 1) {
				output += temp++;

			}
			temp -= 2;
			while (temp >= startingNumber) {
				output += temp--;
			}

		} 
		//the else part is for the lower half of the pyramid
		else {

			int temp2 = startingNumber;
			while (temp2 <= middleNumber) {
				output += temp2++;
			}
			temp2 -= 2;
			while (temp2 >= startingNumber) {
				output += temp2;
				temp2--;
			}

			middleNumber -= 1;
		}
		output += "\n";
		return output;
	}

	/*calculates the spaces's  string in a particular row */
	public String spaces(int rows, int n) {
		String output = "";
		int spaceCount = 0;
		int totalSpacesInEachRow = 0;
		
		//the if part is for the upper half of the pyramid
		if (n < rows) {
			totalSpacesInEachRow = totalCharactersInARow - ((2 * n) - 1);
			while (spaceCount < totalSpacesInEachRow / 2) {
				output += " ";
				spaceCount++;
			}
		} 
		//the else part is for the lower half of the pyramid
		else {
			output += " ";
			int middleNumber = totalRowsInPattern - n;
			totalSpacesInEachRow = totalCharactersInARow
					- ((2 * middleNumber) - 1);
			spaceCount = 0;
			while (spaceCount < totalSpacesInEachRow / 2) {
				output += " ";
				spaceCount++;
			}

		}
		return output;

	}

}
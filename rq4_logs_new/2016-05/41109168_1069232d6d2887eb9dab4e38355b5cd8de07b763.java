package gears;

import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter Gears");
		String input = scanner.nextLine();
		String[] gears = input.split("T");
		double ratio = Double.parseDouble(gears[0])/Double.parseDouble(gears[gears.length-1]);
		System.out.printf("The final gear will move %.0f rotations",ratio*10);
	}

}
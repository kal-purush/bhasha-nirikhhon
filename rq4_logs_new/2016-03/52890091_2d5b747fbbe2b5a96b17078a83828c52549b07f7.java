package com.colleageboard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;

public class FindExpectedNumber {

	/**
	 * To capture the number of attempts, we get the expected number
	 */
	private int numAttempptCounter = 0;

	/**
	 * Buffered Reader to read the input from the user
	 */
	private BufferedReader br = null;

	/**
	 * Default Constructor - creates instance of a buffer reader to read user
	 * input
	 */
	public FindExpectedNumber() {
		super();
		try {
			br = new BufferedReader(new InputStreamReader(System.in));
		} catch (final Exception e) {
			System.err.println("Exception while creating a BufferedReader: " + e);
		}
	}

	/**
	 * This method used to find the user expected number. It users the binary search
	 * algorithm. I while loop get expected as long as you find the expected number,
	 * exits when you enter YES
	 * 
	 * @param lowerInput
	 * @param higherInput
	 * @throws IOException
	 */
	public void findNumber(int lowerInput, int higherInput) throws IOException {

		while (true) {
			int expectedNumber = (lowerInput + higherInput) / 2;
			numAttempptCounter++;
			System.out
					.println("Please enter one of the choice( HIGHER, LOWER, YES)");
			System.out.println("Is your Expected Number: " + expectedNumber);
			String userEnteredValue = br.readLine();
			
			if(null != userEnteredValue && userEnteredValue.trim().length() > 0) {
				if (userEnteredValue.equalsIgnoreCase(NumberFinderInterface.HIGHER)) {
					lowerInput = expectedNumber + 1;
				} else if (userEnteredValue
						.equalsIgnoreCase(NumberFinderInterface.LOWER)) {
					higherInput = expectedNumber - 1;
				} else if (userEnteredValue
						.equalsIgnoreCase(NumberFinderInterface.YES)) {
					System.out.println("I Found it, in " + numAttempptCounter
							+ " attemtps");
					break;
				} else {
					System.out.println("You entered invalid choice");
				}
			} else {
				System.out.println("You entered invalid choice");
			}
		}
	}

	/**
	 * Main method to start execute the program
	 * @param args
	 */
	public static void main(final String[] a) {

		FindExpectedNumber findExpectedNumber = new FindExpectedNumber();

		System.out.println("Choose a random number (integer) between "
				+ NumberFinderInterface.MIN_NUMBER + " and "
				+ NumberFinderInterface.MAX_NUMBER);

		try {

			findExpectedNumber.findNumber(NumberFinderInterface.MIN_NUMBER,
					NumberFinderInterface.MAX_NUMBER);
		} catch (IOException e) {
			System.err.println("Exception while creating a Reading the user input: " + e);
		} finally {
			findExpectedNumber.closeBufferedReader();
		}

	}
	
	/**
	 * if br is not null, then closes the buffered reader
	 */
	public void closeBufferedReader() {

		if (null != br) {
			try {
				br.close();
			} catch (final Exception ex) {
				System.err.println("Exception while closing the BufferReader");
			}
		}
	}

}
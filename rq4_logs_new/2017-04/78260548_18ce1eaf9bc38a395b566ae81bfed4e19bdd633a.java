package com.tutorial.tests;

import java.io.*;
import java.util.Scanner;

public class GoslinMelissaUnit5 {

public static void main(String[] args) {

    Scanner stdIn = new Scanner(System.in);

    final String HEADING_FMT_STR = "%5s%9s%13.8s%11s%11s%11s \n";   // headers
    final String DATA_FMT_STR = "%5d%7d%13.2f%14.2f%11.2f%11.2f \n";    // data received 

    String maxRandomNumStr;  // max random #
    int maxRandomNumInt;     // max random number after converted
    String roundsStr;        // rounds after converted
    int roundsInt;           // number of rounds

    int randomNumber;        // random number
    double fraction;        // fraction of random #
    double cubed;           // random number cubed
    double sqrt;            // square root of random #
    double log;             // log of random #



    System.out.print("Enter a maximum random number. (Example: 1-50) ");
    maxRandomNumStr = stdIn.next();

    maxRandomNumInt = Integer.parseInt(maxRandomNumStr);   // wrapper class convert string to integer 

    System.out.print("Enter the number of rounds (Example: 1-20) ");
    roundsStr = stdIn.next();

    roundsInt = Integer.parseInt(roundsStr);   // wrapper class convert string to integer

    System.out.printf("\n" + HEADING_FMT_STR,
            "Round", "Rand #", "Fraction", "x^3", "Sqrt", "Log");    // headers


    for (int rounds = 1; rounds <= roundsInt; rounds++)
    {

    randomNumber = (int) (Math.random() * maxRandomNumInt + 1);
    fraction = maxRandomNumInt / randomNumber;
    cubed = randomNumber * randomNumber * randomNumber;
    sqrt = (Math.sqrt(randomNumber));
    log = (Math.log(randomNumber));

    System.out.printf("\n" + DATA_FMT_STR,                          // data
            rounds, randomNumber, fraction, cubed, sqrt, log);

    } // end for loop


    System.out.println("The minimum generated number is: ");
    System.out.println("The maximum generated number is: ");
    System.out.println("The total of generated numbers is: ");
    System.out.println("The average of generated numbers is: ");

}  // end main
} // end class GoslinMelissaUnit5
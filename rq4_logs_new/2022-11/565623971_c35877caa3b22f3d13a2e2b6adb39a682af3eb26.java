package com.yearup.week2;
import java.util.Scanner;

public class ControlStructuresExercise {
    public static void main(String[] args) {
        boolean isContinue;
        String userContinueResponse;
        Scanner sc = new Scanner(System.in);
        do {
            System.out.println("For which planet do you want to know the number of moons?");
            String planetName = sc.nextLine();
            switch (planetName) {
                case "Earth":
                    System.out.println("Earth has 1 moon.");
                    break;
                case "Mars":
                    System.out.println("Mars has 2 moons.");
                    break;
                default:
                    System.out.println("The file does not have information on planet " + planetName + ".");
                    break;
            }
            System.out.println("Would you like to continue(yes/no)?");
                Scanner sn = new Scanner(System.in);
                userContinueResponse = sn.nextLine();
            if (userContinueResponse.equals( "yes")) {
                isContinue = true;
            } else
                isContinue =false;
        }
            while (isContinue) ;

    }
}

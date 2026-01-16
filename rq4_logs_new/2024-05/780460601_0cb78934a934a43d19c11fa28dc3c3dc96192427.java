package com.hishamfactory;

import java.util.Scanner;

public class PilotsController {
    Scanner sc = new Scanner(System.in);
    public boolean showPilotMenu(Company company){
        boolean flag = true;
        while(flag){
            System.out.println(".........................Pilot Menu...........................");
            System.out.println("1.add new pilot");
            System.out.println("2.Quit");
            System.out.print("Enter a choice: ");
            int option = sc.nextInt();
            sc.nextLine();
            switch (option){
                case 1:
                    company.addPilot(company);
                    break;
                case 2:
                    flag = false;
                    break;
                case 3:
                    System.out.println("*** Please choose a a valid choice *** ");
            }
        }
        return flag;
    }
}
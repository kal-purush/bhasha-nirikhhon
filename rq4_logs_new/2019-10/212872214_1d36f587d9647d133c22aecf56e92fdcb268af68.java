package com.company;

import java.util.Scanner;

public class Accounting extends Employeer {
    Scanner input = new Scanner(System.in);
    String lastedcer;

    public void last_educational_certificate(){
        System.out.println("Enter Last Educational Certificate :");
        lastedcer =input.nextLine();
    }
}
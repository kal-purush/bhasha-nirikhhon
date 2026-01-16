package com.developer.rmi.app;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Scanner;

import com.developer.rmi.interfaces.Calculator;

public class Client {
    public static void main(final String args[]) throws MalformedURLException, NotBoundException {

        try {

            Calculator calculator = (Calculator) Naming.lookup("rmi://localhost:3000/calculator");

            System.out.println("\nEnter a number: ");

            Scanner input = new Scanner(System.in);
            long number = input.nextInt();
            
            System.out.printf("\nResult factorial: " + calculator.factorial(number));
        
        } catch (RemoteException error) {}
    }
}
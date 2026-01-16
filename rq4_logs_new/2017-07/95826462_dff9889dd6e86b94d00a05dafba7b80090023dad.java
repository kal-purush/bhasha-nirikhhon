
import java.util.Scanner;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author harun
 */
public class TransactionProcess {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //Initialize User Input
        Scanner in = new Scanner(System.in);
      
        //list variables
        double balance;
        double transferBalance;
         //Initialize variables
        balance = 0;
        transferBalance = 0;
        
            //reject negative balance
           if(balance < 0){
                System.out.println("Account Overdrawn");
                balance += balance;
           } else {
            //get balance
                System.out.println("Your balance is: " + "£" +  balance);
           }
            //Increase balance by user input
         
            System.out.print("Enter the amount you would like to deposit: £");
            double amount;
            amount = in.nextDouble();
                   
            //Compute deposit
            
            while ((amount <= 0) || (amount <5)){
                System.out.println("Enter an amount greater than £5");
                amount = in.nextDouble();
                balance += balance;
            } if(amount >= 5){
                System.out.println("Balance: " + "£" + balance);              
            }
            
            //Withdraw a given amount
            System.out.println("Enter amount you would like to withdraw: ");
            double removeAmount;
            removeAmount = in.nextDouble();
            while(balance < removeAmount)
            {
                System.out.println("Insufficient funds");
                System.out.println("Enter amount you would like to withdraw: ");
                removeAmount = in.nextDouble();    
                balance -=removeAmount;
            }
            System.out.println("Your new balance is: " + "£" + balance);
            
            //Send given amount to new balance
            System.out.println("Enter amount you would like to send: ");
            double transferAmount;
            transferAmount = in.nextDouble();
            while(balance < transferAmount)
            {
                System.out.println("Insufficient funds");
                System.out.println("Enter amount you would like to withdraw: £");
                transferAmount = in.nextDouble();
                transferBalance += transferAmount;
                System.out.println("You have transfered: " + "£" + transferBalance);
            }
            System.out.println("Account balance: " + "£" + balance);
            System.out.println("Transfer balance: " + "£" + transferBalance);
    }
    
}
import java.util.Scanner;  
//create ATMExample class to implement the ATM functionality  
public class ATM  
{  
    //main method starts   
    public static void main(String args[] )  
    {  
        //declare and initialize balance, withdraw, and deposit  
        int balance = 100000, withdraw, deposit;  
          
        //create scanner class object to get choice of user  
        Scanner sc = new Scanner(System.in);  
          
        while(true)  
        {  
            System.out.println("Automated Teller Machine");  
            System.out.println("1. View Availiable Balance ");  
            System.out.println("2. Withdraw Amount");  
            System.out.println("3. Deposit Amount");  
            System.out.println("4. View Ministatement ");  
            System.out.println("5. Exit ");   
            System.out.print(" Enter Choice : ");  
            //get choice from user  
            int choice = sc.nextInt();  

            switch(choice)  
            {  
            case 1:  
                //displaying the total balance of the user  
                System.out.println("Balance : "+balance);  
                System.out.println("");  
                break;  
            case 2:  
                System.out.println("Enter ATM Pin : "); 
                //get pin from user 
                int authorized_pin=123;
                int pin = sc.nextInt(); 
                if (authorized_pin==pin)
                {
                    System.out.print("Account Authorized! ");
                    System.err.println(" ");
                    System.out.print("Enter money to be withdraw ");  
                    //get the withdrawl money from user  
                    withdraw = sc.nextInt();            
                    System.out.println("Confirm? Y/N");
                    
                    Scanner s = new Scanner(System.in); 
                    String ch = s.next();
                    switch(ch)
                    {
                         case "Y"://check whether the balance is greater than or equal to the withdrawal amount 

                         if(balance >= withdraw)  
                         {  
                             //remove the withdrawl amount from the total balance  
                             balance = balance - withdraw;  
                             System.out.println("Collect the Cash "+withdraw);  
                             System.out.println(" ");
                             System.out.println("Loading Account Balance............");
                             System.out.println("Your Current Balance is : "+ balance);
                         }  
                         else  
                         {  
                             //show custom error message   
                             System.out.println("Insufficient Balance");  
                         }  
                         break;
                        case "N":
                        System.exit(0); 
                        break;
                    }
                    
                }
                else 
                System.out.print("Account Unauthorized! ");
                System.out.println("");  
                break;  
   
            case 3:  
                System.out.print("Enter money to be deposited:");             
                //get deposite amount from te user  
                deposit = sc.nextInt();  
                //add the deposit amount to the total balanace  
                balance = balance + deposit;  
                System.out.println("Your Money has been successfully depsited");  
                System.out.println("");  
                break;  
   
            case 4:  
                //ministatement : previous 5 transition details  
                System.out.println("Balance : "+balance);  
                System.out.println("");  
                break;  
   
            case 5:  
                //exit from the menu  
                System.exit(0);  
            default:
                //default statement
                System.out.println("Invalid option. Please choose again.");
            }  
        }  
    }  
}  
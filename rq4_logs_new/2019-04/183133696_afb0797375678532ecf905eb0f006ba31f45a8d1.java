package atm.view;

import atm.controller.MainController;
import atm.model.Account;
import atmmvc.DepositSlot;
import atmmvc.Keypad;
import atmmvc.Screen;

public class Deposit implements View
 {  
   private double amount;
   private Keypad keypad;
   private DepositSlot depositSlot;
   private Account account;
   private Screen screen;
   private int accountNumber;
   private MainController controller;
   private final static int CANCELED = 0;

    public Deposit(Keypad keypad, DepositSlot depositSlot, Account account, Screen screen, MainController controller, Account account1) {
        this.keypad = keypad;
        this.depositSlot = depositSlot;
        this.account = account;
        this.screen = screen;
        this.controller = controller;
        this.account = account;
    }
           
   public void show(){            
     amount = promptForDepositAmount();
     if(amount != CANCELED){         
       screen.displayMessage("\nPlease insert a deposit envelope containing ");  
       screen.displayDollarAmount(amount);  
       screen.displayMessage(".");         
       boolean envelopeReceived = depositSlot.isEnvelopeReceived();         
       if(envelopeReceived){  
         screen.displayMessageLine("\nYour envelope has been received.");  
         screen.displayMessage("NOTE: The money just deposited will not be available until we verify the amount");  
         screen.displayMessage("of any enclosed cash and your checks clear.");           
         controller.credit(accountNumber, amount);
       }
       else{           
         screen.displayMessageLine("\nYou did not insert an envelope");  
         screen.displayMessageLine("So, the ATM has canceled your transaction.");  
       }
     }
     else{  
      
       screen.displayMessageLine("\nCanceling transaction...");  
     }
   }
   
   private double promptForDepositAmount(){       
     screen.displayMessage("\nPlease enter a deposit amount in CENTS (or 0 to cancel)");  
     int input = keypad.getInput(); // receive input of deposit amount  
     // check whether the user canceled or entered a valid amount  
     if(input == CANCELED) return CANCELED;  
     else{  
       return (double) input / 100;    // return dollar amount  
     }
   }

    @Override
    public void askCommand() {
        controller.credit(accountNumber, amount);
    }
   
   
   }
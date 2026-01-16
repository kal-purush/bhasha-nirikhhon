/**
 *
 * @author ricardo
 */


package diceroll;

import java.util.Scanner;
import java.util.Random;

public class DiceRoll {

    
    public static void main(String[] args) {
        
        Scanner scannerDice= new Scanner(System.in);
        
                 System.out.print("WELCOME TO DICE ROLLER!"
                      + "!\n");
         
                 System.out.print("PRESS 'R' to ROLL the DICE.\n");
  
                 String Ddice = scannerDice.nextLine();//input for rolling
   
   while(Ddice.equalsIgnoreCase("r")){
       
    if (Ddice.equalsIgnoreCase("r")){
     //random number 1-6
        Random rand = new Random();
        int Dicenum = rand.nextInt((6-1)+1)+1;
        
        
        
         System.out.print("You rolled a "+ Dicenum +"! \n");
     
         printdice(Dicenum); //prints dice 
      
       System.out.print("press 'r' again to reroll! press any key other than 'r' to quit \n");
        Ddice = scannerDice.nextLine();
        
   } 
    }
   
    if (!Ddice.equals("r")){ 
       System.out.print("\n BYE.Thanks for playing.\n");
            }
    }
    
    static void printdice(int numshow){
        switch (numshow) {
            case 1:
                System.out.print("/////////////\n"
                        +       "/           /\n"
                        +      "/     o     /\n"
                        +     "/           /\n"
                        +    "/////////////\n");
                break;
            case 2:
                System.out.print("/////////////\n"
                        +       "/        o  /\n"
                        +      "/           /\n"
                        +     "/  o        /\n"
                        +    "/////////////\n");
                break;
            case 3:
                System.out.print("/////////////\n"
                        +       "/ o         /\n"
                        +      "/     o     /\n"
                        +     "/        o  /\n"
                        +    "/////////////\n");
                break;
            case 4:
                System.out.print("/////////////\n"
                        +       "/  o     o  /\n"
                        +      "/           /\n"
                        +     "/  o     o  /\n"
                        +    "/////////////\n");
                break;
            case 5:
                System.out.print("/////////////\n"
                        +       "/  o     o  /\n"
                        +      "/     o     /\n"
                        +     "/  o     o  /\n"
                        +    "/////////////\n");
                break;
            case 6:
                System.out.print("/////////////\n"
                        +       "/  o     o  /\n"
                        +      "/  o     o  /\n"
                        +     "/  o     o  /\n"
                        +    "/////////////\n");
                break;
            default:
                break;
        }
            
        }


}
    
    
    

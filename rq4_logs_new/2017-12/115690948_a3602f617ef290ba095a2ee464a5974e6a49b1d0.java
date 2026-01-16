/**
 *
 * @author ricardo
 */
package diceroll;
import java.util.Scanner;

public class Dice {
    
 public static void main(String[] args) {
        // TODO code application logic here
        Scanner scannerDice= new Scanner(System.in);
        System.out.print("WELCOME TO DICE FATE!"
                + "!\n");
         System.out.print("PRESS 'R' to ROLL the DICE!\n");
   String Ddice = scannerDice.nextLine();
   while(Ddice.equals("r")){
   if (Ddice.equals("r")){
       int Dicenum =(int)(Math.random()*((6-1)+1))+1;
       System.out.print("You rolled a "+ Dicenum +"! \n");
       printdice(Dicenum);
       
       System.out.print("press 'r' again to reroll! press any key other than 'r' to quit \n");
        Ddice = scannerDice.nextLine();
        
   } 
    }if (!Ddice.equals("r")){ 
       System.out.print("\n BYE.Thanks for playing!\n");
            }
    }static void printdice(int numshow){
        if (numshow == 1){
        System.out.print("/////////////\n"
                +       "/           /\n"
                +      "/     o     /\n"
                +     "/           /\n"
                +    "/////////////\n");}
        else if(numshow ==2){
        System.out.print("/////////////\n"
                +       "/        o  /\n"
                +      "/           /\n"
                +     "/  o        /\n"
                +    "/////////////\n");
        }else if(numshow ==3){
        System.out.print("/////////////\n"
                +       "/ o         /\n"
                +      "/     o     /\n"
                +     "/        o  /\n"
                +    "/////////////\n");
        }else if(numshow ==4){
        System.out.print("/////////////\n"
                +       "/  o     o  /\n"
                +      "/           /\n"
                +     "/  o     o  /\n"
                +    "/////////////\n");
        }else if(numshow ==5){
        System.out.print("/////////////\n"
                +       "/  o     o  /\n"
                +      "/     o     /\n"
                +     "/  o     o  /\n"
                +    "/////////////\n");
        }else if(numshow ==6){
        System.out.print("/////////////\n"
                +       "/  o     o  /\n"
                +      "/  o     o  /\n"
                +     "/  o     o  /\n"
                +    "/////////////\n");
        }
            
        }


}
    
    
    

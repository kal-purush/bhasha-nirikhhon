package View;
import java.util.Date;
public class Advice {
    //method overloading
    static void adviceGiven() {
        Date date = new Date();
        //Prints today's date
        System.out.println("Today is: " + date);
        //Asks the user their names and creates a username using a random two-digit numbers
        System.out.println("Hello! Welcome to the MoneyUP! The budgeting app!" +"\n");
                Advice();
                System.out.println(Advice(15,20,25,40));
            }
            public static void Advice() {
                System.out.println("Here is some expert advice before you begin." + "\n" + "-----------------------------------------------------------------------------------------");
                System.out.println("Advice 1: Make sure to write out your budget before you get started. " + "\n" + "-----------------------------------------------------------------------------------------");
                System.out.println("Advice 2: Do not spend all of your money if it is not needed, because this will go into your savings. " + "\n" +"-----------------------------------------------------------------------------------------" );
                System.out.println("Advice 3: If you know your income is constant try to keep your important expenses fixed." + "\n"+ "-----------------------------------------------------------------------------------------");
            }
            public static String Advice ( int recommendedFood, int recommendedShop, int recommendedTrans, int recommendedSave) {
                return ("Before you get started we generally recommend beginners to allocate " + recommendedFood + "%" + " on food, " + recommendedShop+ "%" + " on shopping, " +recommendedTrans+ "%" + " on transportation, and "+ recommendedSave+ "%" + " on savings" + "\n" + "Thank you, lets begin !!" );
            }
        }
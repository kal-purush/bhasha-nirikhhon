/*
10/02/20
 */
//Author: Edgar
package PK_EDGAR;
//Libraries:

import java.util.Scanner;
import java.text.DecimalFormat;
import java.util.Arrays;

public class P17 {
    //Global declarations:

    static Scanner keyboard = new Scanner(System.in);

    public static void main(String[] args) {
        int option = -1;
        keyboard.useDelimiter("\n");
        do {
            userMenu();
            option = keyboard.nextInt();
            switch (option) {//init of switch 
                case 1:
                    System.out.println("Option 1");
                    int eu;
                    float res;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eu = keyboard.nextInt();
                    res = conversorEdgar(eu);
                    conversorEdgar(eu);
                    System.out.println(res);
                    break;
                case 2:
                    System.out.println("Option 2");
                    int eur;
                    double rest;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eur = keyboard.nextInt();
                    rest = conversorEdgar2(eur);
                    conversorEdgar2(eur);
                    System.out.println(rest);
                    break;
                case 3:
                    System.out.println("Option 3");
                    int eur3;
                    String rest3;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eur3 = keyboard.nextInt();
                    rest3 = conversorEdgar3(eur3);
                    conversorEdgar3(eur3);
                    System.out.println(rest3);
                    break;
                case 4:
                    System.out.println("Option 4");
                    String eur4;
                    int rest4;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eur4 = keyboard.next();
                    rest4 = conversorEdgar4(eur4);
                    conversorEdgar4(eur4);
                    System.out.println(rest4);
                    break;
                case 5:
                    System.out.println("Option 5");
                    float eur5;
                    int rest5;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eur5 = keyboard.nextFloat();
                    rest5 = conversorEdgar5(eur5);
                    conversorEdgar5(eur5);
                    System.out.println(rest5);
                    break;
                case 6:
                    System.out.println("Option 6");
                    float eur6;
                    String rest6;
                    //preguntar euro;
                    System.out.println("euros: ");
                    eur6 = keyboard.nextFloat();
                    rest6 = conversorEdgar6(eur6);
                    conversorEdgar6(eur6);
                    System.out.println(rest6);
                    break;
                case 7:
                    System.out.println("Option 7");
                    System.out.println("Letter ?:");
                    char letter = (keyboard.next()).charAt(0);
                    charToBinary(letter);
                    break;
                case 8:
                    System.out.println("Option 8");
 
                    System.out.println("Word ?:");
                    String str=keyboard.next();
                    String strResult=StringToBinary(str);
                    System.out.println(strResult);
                    break;
                case 9:
                    System.out.println("Option 9");
                    
                    System.out.println("Number between 0-255 ?:");
                    int num=(keyboard.nextInt());
                    IntToBinary(num);
                    break;
                case 10:
                    apart10();
                    break;
                case 11:
                    p11();
                    break;
                default:
                    System.out.println("Invalid Option...");
            }

        } while (option != 0);
    }

    private static void charToBinary(char ch) {
        String letterBinary = Integer.toBinaryString(ch);
        System.out.println(ch + "=" + letterBinary);
 
    }

    private static void userMenu() {
        System.out.print("\n");
        System.out.print("\n");
        System.out.print("\n");
        System.out.println("Option1:(Numeros de 0 a 11):");
        System.out.println("Option2:");
        System.out.println("Option3:");
        System.out.println("Option4:");
        System.out.println("Option5:");
        System.out.println("Option6:");
        System.out.println("Option7:");
        System.out.println("Option8:");
        System.out.println("Option9:");
        System.out.println("Option10:");
        System.out.println("Option11:");
        System.out.println("Option0: (exit):");

        System.out.println("\n\noption ?");
    }

    //int-float
    private static float conversorEdgar(int euro) {
        float result = 0;
        result = (float) euro / 0.91f; //cast
        // result= Float.valueof(euro)/ 0.91;
        return result;
    }

    private static double conversorEdgar2(int euroo) {
        double result = 0;
        result = (double) euroo / 0.91; //cast
        // result= Double.valueof(euro)/ 0.91;
        return result;

    }

    private static String conversorEdgar3(int euro3) {
        DecimalFormat df = new DecimalFormat("0.000");
        String result = "0";
        result = String.valueOf(euro3 / 0.91); //cast

        return df.format(result);
    }

    private static int conversorEdgar4(String euro4) {
        int result = 0;
        result = (int) (Integer.parseInt(euro4) / 0.91); //cast
        return result;
    }

    private static int conversorEdgar5(float euro5) {
        int result;
        result = (int) (euro5 / 0.91); //cast

        return result;
    }

    private static String conversorEdgar6(float euro6) {
        String result;
        result = String.valueOf(euro6 / 0.91); //cast

        return result;
    }


    private static String StringToBinary(String str) {
    String result="";
    
    char ch=' ';
    for(int i=0; i<str.length();i++){
    ch=str.charAt(0);
    result += Integer.toBinaryString(ch)+" ";
    }
         
    return result;
    }

    private static void IntToBinary(int num) {
    int number1 = 0;
    char ch2= (char) number1;
     System.out.println(number1);
    
    
    }

    private static void apart10() {
        System.out.println("----------");
        for(int i=0;i<255;i++){
            System.out.println(i+"\t"+Integer.toString(i,16)+"="+(char)i);
        }
    }

    private static void p11() {
        double preu = 100;
        double tax = 21;
        double res = function(preu, tax); //llamar a function
        System.out.println(res); //llamar a procedimiento
        method(preu, tax, res);

    }

    //function CalculoPVP
    private static double function(double price, double iva) {
        double PVP = 0; //declarar la variable de return
        PVP = price + price * (iva / 100);
        return PVP;
    }
    public static final String PURPLE = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String RED = "\u001B[31m";

    public static void method(double price, double iva, double res) {

        System.out.println(RED + "Price=" + price);
        System.out.println(CYAN + "IVA=" + iva);
        System.out.println(PURPLE + "Result=" + res);

    }
}
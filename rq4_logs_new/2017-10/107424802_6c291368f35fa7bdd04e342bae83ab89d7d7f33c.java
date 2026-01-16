import java.util.Scanner;
public class DiceToss{
    public static void main(String []args){
        Scanner in=new Scanner(System.in);
        System.out.println("This program will simulate tossing a pair of dice.");
        System.out.print("How many times would you like the dice to be tossed? ");
        int numberRequest=in.nextInt();
        int two=0;
        int three=0;
        int four=0;
        int five=0;
        int six=0;
        int seven=0;
        int eight=0;
        int nine=0;
        int ten=0;
        int eleven=0;
        int twelve=0;
        for(int i=0;i<numberRequest;i++){
            int random=(int)(Math.random() * (13 - 2) + 2);
            switch(random){
                case 2: two+=1;
                break;
                case 3: three+=1;
                break;
                case 4: four+=1;
                break;
                case 5: five+=1;
                break;
                case 6: six+=1;
                break;
                case 7: seven+=1;
                break;
                case 8: eight+=1;
                break;
                case 9: nine+=1;
                break;
                case 10: ten+=1;
                break;
                case 11: eleven+=1;
                break;
                case 12: twelve+=1;
                break;
            }
        }
        System.out.print("2: ");
        for(int i=0;i<two;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("3: ");
        for(int i=0;i<three;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("4: ");
        for(int i=0;i<four;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("5: ");
        for(int i=0;i<five;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("6: ");
        for(int i=0;i<six;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("7: ");
        for(int i=0;i<seven;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("8: ");
        for(int i=0;i<eight;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("9: ");
        for(int i=0;i<nine;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("10: ");
        for(int i=0;i<ten;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("11: ");
        for(int i=0;i<eleven;i++){
            System.out.print("*");
        }
        System.out.println();
        System.out.print("12: ");
        for(int i=0;i<twelve;i++){
            System.out.print("*");
        }
    }
}
      
import java.util.Formatter;
import java.util.Scanner;

public class ControlFlowExercises {
    public static void main(String[] args) {
//  While Loop
        int i = 5;
        while (i <= 15){
            System.out.print(i + " ");
            i++;
        }
//  Do-While Loops
        int j = 0;
        do {
            System.out.println(j);
            j += 2;
        }while ( j <= 100);

        int k = 100;
        do {
            System.out.print(k + " ");
            k -= 5;
        }while (k >= -10);

        System.out.println();

        int m = 2;
        do {
            System.out.println(m);
            m = (int) Math.pow(m, 2);
        }while (m < 1000000);

//  For Loops
        for (int x = 5; x <= 15; x++){
            System.out.print(x + " ");
        }

        System.out.println();

        for(int y = 0; y <= 100; y+=2){
            System.out.println(y);
        }

        for(int z = 100; z>=-10; z -= 5){
            System.out.print(z + " ");
        }

        System.out.println();

        for(long a = 2; a < 1000000; a*=a){
            System.out.print(a + " ");
        }

        System.out.println();

//  Fizzbuzz
        for (int l = 1; l <= 100 ; l++) {
           if(l%15==0){
               System.out.println("Fizzbuzz!");
           }else if(l%5==0){
               System.out.println("Buzz!");
           }else if(l%3==0){
               System.out.println("Fizz!");
           }else{
               System.out.println(l);
           }
        }

//    Table of Powers
        Scanner userInput = new Scanner(System.in);
        System.out.println("The following table will display the squared and cubed values up to a given integer.");
        System.out.println("Enter an integer: ");
        int chosenNumber = Integer.parseInt(userInput.next());
        Formatter f = new Formatter();
        f.format("%-6s %1s %-7s %1s %-6s\n", "Number", "|", "Squared", "|", "Cubed");
        f.format("%-6s %1s %-7s %1s %-6s\n", "------", "|", "-------", "|", "-----");
        for (int l = 1; l <= chosenNumber ; l++) {
            f.format("%-6s %1s %-7s %1s %-6s\n", l, "|", (int)Math.pow(l, 2), "|", (int)Math.pow(l, 3));
        }
        System.out.println(f);

        boolean keepGoing = false;
        int gradeNum = 0;
        char letterGrade = ' ';
        String anotherScore = " ";
        do{
           System.out.println("Please enter a numerical grade (0-100): ");
           gradeNum = Integer.parseInt(userInput.next());
           if(gradeNum >=88){
               letterGrade = 'A';
           }else if(gradeNum <= 87 & gradeNum >=80){
               letterGrade = 'B';
           }else if(gradeNum <= 79 & gradeNum >=67){
               letterGrade = 'C';
           }else if(gradeNum <= 66 & gradeNum >= 60){
               letterGrade = 'D';
           }else if(gradeNum <= 59){
               letterGrade = 'F';
            }
           System.out.println("Your grade translates to a " + letterGrade);
           System.out.println("Would you like to enter another score? (y=yes, n=no)");
           anotherScore = userInput.next();
           if(anotherScore.equalsIgnoreCase("y")){
               keepGoing = true;
           }else{
               keepGoing = false;
           }
        }while(keepGoing);
    }
}
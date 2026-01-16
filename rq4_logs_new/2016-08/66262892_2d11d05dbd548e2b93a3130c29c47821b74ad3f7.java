package classExercises;
import java.util.Scanner;
/**
 * Created by jjenkins on 8/30/2016.
 */
public class ComputeLine {

    public static void main (String[] args){

        Scanner input = new Scanner( System.in);

        //obtain yearly interest rate
        System.out.print("Enter a yearly interest rate, eg 8.25: ");
        double annualIntRate = input.nextDouble();

        //obtain monthly interest rate
        double monthlyIntRate = annualIntRate/1200;

        //obtain num of years
        System.out.print("Enter number of years as int, eg. 5: ");
        int numOfYears = input.nextInt();

        //obtain loan amount
        System.out.print("Enter loan amount, eg. 120000.95:  ");
        double loanAmount = input.nextDouble();

        // calc loan payment
        double monthPayment = loanAmount * monthlyIntRate / (1 - 1 /Math.pow(1+monthlyIntRate, numOfYears * 12));

        //total loan repayment
        double totalPayment = monthPayment * numOfYears *12;

        //typecasting to an int to get rid of decimal places the devide by 100.0 because we want it on dollars and cents
        System.out.println("The monthly payment is "+ (int)(monthPayment * 100) / 100.0);
        System.out.println("The total monthly payment is "+ (int)(totalPayment * 100) / 100.0);

    }
}
import java.util.Scanner;

public class ErdemCodes {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        System.out.print("Loan Amount: ");
        double loanAmount = input.nextDouble();
        System.out.print("Number of Years: ");
        int years = input.nextInt();
        System.out.print("Annual Interest Rate: ");
        double annualRate = input.nextDouble();

        double monthlyInterestRate = annualRate / 1200;

        double monthlyPayment = loanAmount * monthlyInterestRate / (1 - 1 / Math.pow(1 + monthlyInterestRate, years * 12));

        System.out.printf("\nMonthly Payment: %.2f\n", monthlyPayment);

        System.out.printf("Total Payment: %.2f\n", (monthlyPayment * 12) * years);

        double balance = loanAmount;
        double principal;
        double interest;

        System.out.println("\nPayment#     Interest     Principal     Balance");

        for (int i = 1; i <= years * 12; i++) {
            interest = monthlyInterestRate * balance;
            principal = monthlyPayment - interest;
            balance = balance - principal;

            System.out.printf("%-13d%-13.2f%-14.2f%.2f\n", i, interest, principal, balance);
        }
    }
}
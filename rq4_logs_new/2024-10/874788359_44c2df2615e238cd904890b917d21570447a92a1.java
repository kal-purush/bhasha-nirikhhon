import java.util.Scanner;
 
public class InterestRates {
    public static double calculatInterest(double principal, double rate, double time) {
        return (principal * rate * time) / 100;
    }
  
    public static double calculateInterest(double principal, double rate, double time, int n) {
        return principal * Math.pow((1 + rate / (n * 100)), n * time) - principal;
    }
    
    public static double calculateInterest(double principal, double rate, double time) {
        double seniorCitizenRate = rate + 0.5;
        return (principal * seniorCitizenRate * time) / 100;
    }
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("How many times we want to be calculate you:");
        int num=sc.nextInt();
 
        for(int i=0;i<num;i++)
        {
        System.out.print("Enter the principal amount: ");
        double principal = sc.nextDouble();
 
        System.out.print("Enter the rate of interest: ");
        double rate = sc.nextDouble();
 
        System.out.print("Enter the time period in years: ");
        double time = sc.nextDouble();
 
        System.out.println("\nChoose the type of interest to calculate:");
        System.out.println("1. Simple Interest");
        System.out.println("2. Compound Interest");
        System.out.println("3. Senior Citizen Interest");
 
        int choice = sc.nextInt();
 
        switch (choice) {
            case 1:
                double simpleInterest = calculateInterest(principal, rate, time);
                System.out.printf("Simple Interest: %.2f\n", simpleInterest);
                break;
 
            case 2:
                System.out.print("Enter the number of times interest is compounded per year: ");
                int n = sc.nextInt();
                double compoundInterest = calculateInterest(principal, rate, time, n);
                System.out.printf("Compound Interest: %.2f\n", compoundInterest);
                break;
 
            case 3:
                double seniorCitizenInterest = calculateInterest(principal, rate, time);
                System.out.printf("Senior Citizen Interest: %.2f\n", seniorCitizenInterest);
                break;
 
            default:
                System.out.println("Invalid choice!");
                break;
                
        }
        }
        sc.close();
    }
}
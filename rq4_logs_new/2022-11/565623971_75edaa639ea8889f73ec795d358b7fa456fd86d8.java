public class QuotientCalculator {
    /*
    1. Get two arguments from command line
    2. convert them into an integer
    3. store in two variables
    4. Divide those two variables
    5. Store the quotient in the third variables
    6. Print the quotient
    */
    public static void main(String[] args) {
        int firstValue =0 ;
        int secondValue =0;
        double quotient = 0;
        firstValue =  Integer.parseInt(args[0]);
        secondValue = Integer.parseInt(args[1]);
        quotient = (double)(firstValue) / secondValue; //Explicit casting
		/* int/int = int
		 double/int = double
		 int/double = double
		 double/double = double
		 */
        System.out.println("The quotient of the two numbers is");
        System.out.println(quotient);
    }
}
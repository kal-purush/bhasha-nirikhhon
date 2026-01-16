package md.tekwill.topic4;

public class Methods {
    static int sum(int firstNumber, int secondNumber){
        int suma = 0;
        if(firstNumber > secondNumber){
            System.out.println("Impossible to sum up the parameters in this range. Wrong range.");
            return suma;
        } else if (firstNumber == secondNumber){
            System.out.println("The first number in the range is identical to the second one.");
            return firstNumber;
        } else {
            for (int x = firstNumber; x <= secondNumber; x++){
                suma = suma + x;
            }
            System.out.println("Successful completion of the FOR cycle.");
        }
        System.out.println("The method <sum> was fully executed to the end.");
        return suma;
    }
    static void sum(double a) {
        double suma = 0;
        if (a > 0) {
            while (a > 0){
                suma = suma + a;
                a--;
            }
            System.out.println("Successful completion of the WHILE cycle.");
            System.out.println(suma);
        } else if (a == 0) {
            System.out.println("Sum is zero.");
        } else {
            System.out.println("Parameter <a> is less than zero.");
        }
    }
}

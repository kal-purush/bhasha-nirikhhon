package main.project13;

public class Factorial {

    public void checkInteger(int enteredInteger)throws NegativeIntegerException, UpperLimitException {
        if (enteredInteger < 1){
            throw new NegativeIntegerException(enteredInteger);
        }

        if (enteredInteger > 170){
            throw new UpperLimitException(enteredInteger);
        }
    }

    public double getFactorial(int positiveInteger){
        double result = 1;
        for (int i = 1; i <=positiveInteger ; i++) {
            result *= i;
        }
        return result;
    }
}
package codingtests.fizzbuzz;


import com.google.common.base.Preconditions;
import java.util.*;


public class FizzBuzzBasic {

    public static void main(String... arguments) {
        Preconditions.checkArgument(arguments.length == 1, "Program must get exactly one argument");
        try{
            int upperBound = Integer.valueOf(arguments[0]);
            for(String converted : new FizzBuzzBasic().getFizzBuzzValuesForNumbers(1,upperBound)){
                System.out.println(converted);
            }
        } catch (NumberFormatException ex){
            throw new IllegalArgumentException("Argument must be a valid number");
        }
    }

    public List<String> getFizzBuzzValuesForNumbers(int firstNumber, int lastNumber){
        List<String> fizzBuzzList = new ArrayList<>();

        for (int number = firstNumber; number <= lastNumber; number++){
            fizzBuzzList.add(convertNumberToFizzBuzz(number));
        }
        return fizzBuzzList;
    }

    public String convertNumberToFizzBuzz(int number){
        Preconditions.checkArgument(number > 0,"Number should be greater than 0!");

        if (number % 15 == 0) return "fizzbuzz";
        if (number % 3 == 0) return "fizz";
        if (number % 5 == 0) return "buzz";

        return String.valueOf(number);
    }


}
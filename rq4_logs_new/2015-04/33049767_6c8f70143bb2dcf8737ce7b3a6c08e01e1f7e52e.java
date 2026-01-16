package com.dop54321.classex2appactivity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by dop54321 on 02/04/2015.
 */
public class Number {
    private String thisNumber;
    private int defaultNumOfDigits = 4;


    public Number() {

    }

    public Number(int NumOfDigits) {
        this.defaultNumOfDigits = NumOfDigits;
    }

    public Number setThisNumber(String thisNumber) {
        this.thisNumber = thisNumber;
        return this;
    }

    /**
     *
     * @return 4 unique digit number as string
     */
    private String RandomGenerator() {
        List<Integer> numbers = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            numbers.add(i);
        }

        Collections.shuffle(numbers);

        String result = "";
        for(int i = 0; i < this.defaultNumOfDigits; i++){
            result += numbers.get(i).toString();
        }
        return result;
    }

    /**
     *
     * @param other
     * @return number of times the gussed digit and the random number digit at same index are
     * equal.
     */
    public int howManyBools(Number other) {
        int boolCounter = 0;
        String otherNumberString = other.getThisNumber();
        for (int i = 0; i < otherNumberString.length(); i++) {
            if (thisNumber.charAt(i)== otherNumberString.charAt(i)){
                boolCounter++;
            }
        }

        return boolCounter;
    }

    public int howManyHits(Number other) throws Exception {

        int almostCounter = 0;
        for (int i = 0; i < other.getThisNumber().length(); i++) {
            String otherNumberString = other.getThisNumber();
            char c = otherNumberString.charAt(i);
            boolean isCurrentCharIsInTheRandomNumber =
                    thisNumber.contains(String.valueOf(c));

            if (isCurrentCharIsInTheRandomNumber
                    && thisNumber.charAt(i)!=c)
                almostCounter++;
        }
        return almostCounter;
    }


    public Number generateNewNumber(){

        String newNumber = RandomGenerator();
        thisNumber =newNumber;


        return this;
    }


    public int getDefaultNumOfDigits() {
        return defaultNumOfDigits;
    }

    public String getThisNumber() {
        return thisNumber;
    }

    public static void main(String[] args) {
        Number number = new Number();
        String s = String.valueOf(number.generateNewNumber().getThisNumber());
        System.out.println(s);

        number.howManyBools(new Number().setThisNumber("1234"));
        int i = number.howManyBools(new Number().setThisNumber("4567"));
        System.out.println(i);
        int i1 = number.howManyBools(new Number().setThisNumber("8945"));
        System.out.println(i1);
        int i2 = number.howManyBools(new Number().setThisNumber("6578"));
        System.out.println(i2);



    }
}
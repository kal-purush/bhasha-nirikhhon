package org.otus.algo;

public class LuckyNumberCounter {

    public static void main(String[] args) {
        String num = args[0];
        if (args.length == 0) {
            System.out.println("Please provide a number as a first arg");
            return;
        }
        System.out.println(getLuckyNumbersCount(Integer.parseInt(num)));
    }

    private static int getLuckyNumbersCount(int digitsToCompare) {
        int upperBound = (int) Math.pow(10, digitsToCompare);
//        int mid = (int) Math.pow(10, digitsToCompare);
        int luckyCounter = 0;
//        int firstHalf;
//        int secondHalf;
        int sum1;
        int sum2;
        //for (int i = mid; i < upperBound; i++) {
        for (int i = 0; i < upperBound; i++) {
//            firstHalf = i / mid;
//            secondHalf = i % mid;
//            if (firstHalf == secondHalf) {
//                luckyCounter++;
//                continue;
//            }
//            if (getSum(mid, firstHalf) == getSum(mid, secondHalf)) {
//                luckyCounter++;
//            }
            sum1 = getSum(upperBound, i);
            for (int j = 0; j < upperBound; j++) {
                if (i == j) {//optimization
                    luckyCounter++;
                    continue;
                }
                sum2 = getSum(upperBound, j);
                if (sum1 == sum2) {
                    luckyCounter++;
                }
            }
        }
//        luckyCounter++; //case all zeros
        return luckyCounter;
    }

    private static int getSum(int upperBound, int i) {
        if (i < 10) {
            return i;
        }
        int sum = 0;
        int digitPointer = upperBound / 10;
        int remainder = i;
        while (digitPointer >= 1) {
            sum += remainder / digitPointer;
            remainder = remainder % digitPointer;
            digitPointer /= 10;
        }
        return sum;
    }


}
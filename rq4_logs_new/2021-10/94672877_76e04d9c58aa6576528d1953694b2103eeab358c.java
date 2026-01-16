package com.kuldeep.algorithims;

import java.util.Arrays;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
public class CountMoves {

    public static void main(String[] args) {
        CountMoves counter = new CountMoves();

        int[] numbers = {3,4,6,6,3};
        System.out.println(counter.countMoves(numbers));
    }

    boolean isAllEquals(int[] numbers) {
        for (int i = 1; i < numbers.length; i++) {
            if (numbers[i-1] != numbers[i]) return false;
        }
        return true;
    }

    void incrementByOne(int[] numbers, int exceptIndex) {
        for (int i = 0; i < numbers.length; i++) {
            if (i == exceptIndex) continue;
            numbers[i] += 1;
        }
    }

    long countMoves(int[] numbers) {
        Arrays.sort(numbers);

        long moves = 0;
        int selectedIndex = numbers.length - 1;

        while (!isAllEquals(numbers)) {
            incrementByOne(numbers, selectedIndex);
            moves++;

            if (selectedIndex - 1 >= 0 && numbers[selectedIndex - 1] > numbers[selectedIndex]) {
                selectedIndex -= 1;
            }else if (selectedIndex + 1 < numbers.length
                    && numbers[selectedIndex + 1] >= numbers[selectedIndex]) {
                selectedIndex += 1;
            }

        }

        return moves;

    }
}
package com.makebono.sortandsearch.search;

import java.util.Arrays;

import com.makebono.sortandsearch.search.searchinterface.Searcher;

/** 
 * @ClassName: Binary search 
 * @Description: Binary search implementation that does binary search
 * @author makebono
 * @date 2017年11月15日 上午8:52:48 
 *  
 */
public class BinarySearch implements Searcher {
    @Override
    public int search(final int data, final int[] input) {
        return search(data, input, 0);
    }

    // To return the correct index out of several recurssion calls, need to parse a begin index for helping. Remember to
    // update it during further calls.
    private int search(final int data, final int[] input, final int begin) {
        int result = Integer.MAX_VALUE;
        if (data > input[input.length - 1] || data < input[0]) {
            System.out.println("The element you are looking for is not in this array.");
        } else {
            if (data > input[input.length / 2]) {
                final int[] newInput = Arrays.copyOfRange(input, input.length / 2, input.length);
                final int newBegin = begin + input.length / 2;

                result = search(data, newInput, newBegin);
            } else if (data < input[input.length / 2]) {
                final int[] newInput = Arrays.copyOfRange(input, 0, input.length / 2);

                result = search(data, newInput, begin);
            } else {
                result = input.length / 2 + begin;
            }
        }
        return result;
    }
}
package leetcode.Y2023.daily.hard;

import org.junit.Test;

import java.util.*;
/*
*
* If we cook some dishes,
they must be the most satisfied among all choices.

Another important observation is that,
we will cook the dish with small satisfication,
and leave the most satisfied dish in the end.

Explanation
We choose dishes from most satisfied.
Everytime we add a new dish to the menu list,
all dishes on the menu list will be cooked one time unit later,
so the result += total satisfaction on the list.
We'll keep doing this as long as A[i] + total > 0.
* */
import static org.junit.Assert.assertEquals;

public class Q1402ReducingDishes {


    public static int maxSatisfaction(int[] satisfaction) {
        int n = satisfaction.length;
        Arrays.sort(satisfaction);
         int res = 0;
         int total = 0;
        for (int i = n - 1; i >= 0 && satisfaction[i] > -total; --i) {
            total += satisfaction[i];
            res += total;
        }
        return res;
    }

    @Test
    public void test(){
       assertEquals(14,maxSatisfaction(new int[]{-1,-8,0,5,-9}));
    }
}
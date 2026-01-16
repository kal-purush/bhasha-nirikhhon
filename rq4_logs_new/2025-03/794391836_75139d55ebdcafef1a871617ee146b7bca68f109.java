package LeetCode75;

import java.util.ArrayList;
import java.util.List;

/**
 * https://leetcode.com/problems/kids-with-the-greatest-number-of-candies
 * */

public class E_KidWithGreatestCandies {
    public static void main(String[] args) {

    }

    public List<Boolean> kidsWithCandies(int[] candies, int extraCandies) {
        List<Boolean> checkMaxList = new ArrayList<>();
        int max = max(candies);
        for(int i = 0; i < candies.length; i++){
            if(candies[i] + extraCandies >= max){
                checkMaxList.add(true);
            }else {
                checkMaxList.add(false);
            }
        }
        return checkMaxList;
    }

    public int max(int[] array){
        int max = array[0];
        for(int i = 0; i < array.length; i++){
            if(max < array[i]){
                max = array[i];
            }
        }
        return max;
    }
}
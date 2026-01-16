package JavaCodePractice;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class GetNeighborsOfPeaks {

    public static int[] getNeighborPeak( int [] nums){
        //using linked hashset for remove duplicated and keep order
        Set< Integer> set = new LinkedHashSet<>();

        for(int i=1; i<nums.length-1; i++){
            if(nums[i]>= nums[i-1] && nums[i]> nums[i+1]){
                set.add(nums[i-1]);//left neighbors
                set.add(nums[i+1]); //right neighbors
            }

        }
        //convert set into int
        int [] result = new int[set.size()];
        int index = 0;
        for(int num: set){
            result[index++] = num;
        }
        return result;
    }

    public static void main(String [] args){
        int[] nums = {10, 40, 25, 3, 22, 80, 56};
        int[] result = getNeighborPeak(nums);

        // Print the result
        System.out.println(Arrays.toString(result));
    }
}
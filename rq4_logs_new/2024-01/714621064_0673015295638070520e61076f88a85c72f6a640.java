package LeetCode_Daily;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Convert_an_Array_Into_a_2D_Array_With_Conditions {

    public static void main(String[] args) {
        int[] nums = {1,3,4,1,2,3,1};

        System.out.println(findMatrix(nums));
    }

    public static List<List<Integer>> findMatrix(int[] nums) {

        List <Integer> numsList = new ArrayList<>();
        List <List<Integer>> list2D = new ArrayList<>();

        for(int i : nums){
            numsList.add(i);
        }

        int index = 0;
        List<Integer> valuesToRemove = new ArrayList<>();


        while(numsList.size() > 0){

            valuesToRemove.clear();
            list2D.add(new ArrayList<>());

            for(int i = 0; i < numsList.size(); i++){
                if(!list2D.get(index).contains(numsList.get(i))){
                    list2D.get(index).add(numsList.get(i));
                    valuesToRemove.add(numsList.get(i));
                }
            }

            for(Integer value : valuesToRemove){
                numsList.remove(value);
            }

            index++;
        }

        return list2D;
    }

}
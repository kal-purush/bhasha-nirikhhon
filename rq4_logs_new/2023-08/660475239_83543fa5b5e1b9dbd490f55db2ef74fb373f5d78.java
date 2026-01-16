package day16arraylists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArrayLists01 {
    public static void main(String[] args) {
        List<Integer> nums = new ArrayList<>();
        nums.add(8);
        nums.add(2);
        nums.add(3);
        nums.add(10);
        nums.add(11);

        Collections.sort(nums); //2,3,8,10,13

        int mindif= nums.get(1)- nums.get(0);


        for (int i=1;i< nums.size();i++)
        {
            if(nums.get(i)- nums.get(i-1)<mindif)
            {
                mindif=nums.get(i)- nums.get(i-1);

                System.out.println(nums.get(i)+" and "+nums.get(i-1));
            }
            if(nums.get(i)- nums.get(i-1)==mindif)
            {
                System.out.println(nums.get(i)+" and "+nums.get(i-1));
            }

        }




    }
}
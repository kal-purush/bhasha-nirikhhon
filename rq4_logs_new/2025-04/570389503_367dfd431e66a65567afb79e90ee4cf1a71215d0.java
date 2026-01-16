package InterviewCoding;

import java.util.ArrayList;
import java.util.List;

public class perfectNumsA {
    public static void main(String[] args) {
        isPerfect(33550336);
   //     isPer(33550336);

    }
    public static void isPerfect(int a)
    {
        int sum=0;
        List<Integer> nums = new ArrayList<>();
        int b = a/2;
        for(int i =1; i<= b;i++)
        {
            if (a % i == 0) {
                nums.add(i);
            }
        }
        System.out.println("this is the perfect numbers divisions: "+nums);
        for(int j =0 ; j<nums.size();j++)
        {
            sum+=nums.get(j);

        }
        if(sum == a)
        {
            System.out.println(a + " is perfect number");
        }
        else {
            System.out.println(a+" is no perfect number");
        }


    }

    public static void isPer (int a)
    {
        int sum=0;
        int b= a/2;
        for (int i = 1; i <= b; i++) {
            if(a %i == 0)
            {
                sum+=i;
            }

        }
        System.out.println(sum);
        if(sum == a )
        {
            System.out.println(a+" is a perfect number" );
        }
        else
        {
            System.out.println(a+" is nto a perfect number");
        }

    }
}
package com.practice.codeforces.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class YesAnotherTwoIntegersProblem {
    static int k[] = {1,2,3,4,5,6,7,8,9,10};
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            String nums = br.readLine();
            String[] arr = nums.split(" ");
            int a = Integer.parseInt(arr[0]);
            int b = Integer.parseInt(arr[1]);
            int moves = countMoves(a, b);
            System.out.println(moves);
            test--;
        }
    }

    private static int countMoves(int a, int b) {
        if (a == b)
            return 0;
        if(a > b){
            int temp = a;
            a = b;
            b = temp;
        }
        int ans = 0;
       int idx = 9;
       while (a != b) {
           int count = 0;
           int low = 0;
           int high = (int) 1e8;
           while (low <= high) {
                int mid = (low+high)/2;
                if (mid == 1 && a + k[idx]*mid > b)
                    break;
                else if (a + k[idx]*mid <= b) {
                    count = Math.max(count, mid);
                    low = mid + 1;
                }
                else
                    high = mid - 1;
           }
           ans += count;
           a += k[idx]*count;
           idx--;
       }
       return ans;
    }
}
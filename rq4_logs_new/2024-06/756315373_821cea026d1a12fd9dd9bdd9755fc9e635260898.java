package Sliding_Window;

import java.util.ArrayDeque;

public class Minimum_Number_of_K_Consecutive_Bit_Flips {
    public static void main(String[] args) {
        int[]a = {0,0,0,1,0,1,1,0};
        int k = 3;
        System.out.println(call(a , k));
        System.out.println(call1(a , k));
    }
    public static int call(int[]a , int k){
        int count = 0 ;

        int i = 0;
        int j = 0;

        while(j < a.length){

            if(j-i+1 == k){
                // we will flip all the elment only if a[i] == 0
                if(a[i] == 0){
                    // flip the bits
                    for(int x  = i ; x<=j ; x++){
                        a[x] = a[x]==0 ? 1 : 0;
                    }
                    count++;
                }
                // move the pointer
                i++;
            }
            j++;
          // TC :- O(n*k); // gives TLE
        }
        return count;
    }
    public static int call1(int[]a  , int k){
//        int ans =0 ;
//        int n = a.length;
//        boolean[]isFlipped = new boolean[n];
//        int flip_count = 0;
//
//        for(int i = 0 ; i<a.length; i++){
//
//            // handling flip_count;
//            if(i >= k && isFlipped[i-k] == true){
//                flip_count--;
//            }
//
//
//            if(flip_count % 2  == a[i]){
//                // do the flip;
//
//                // boundary condition
//                if(i+k > n )return -1;
//
//                flip_count++;
//                ans++;
//                isFlipped[i] = true;
//
//            }
//        }
//        return ans;
        int n = a.length;

        int flips = 0;
        boolean[] isFlipped = new boolean[n];
        int flipCountFromPastForCurri = 0;

        for (int i = 0; i < n; i++) {
            if (i >= k && isFlipped[i - k]) {
                flipCountFromPastForCurri--;
            }

            if (flipCountFromPastForCurri % 2 == a[i]) {
                if (i + k > n) {
                    return -1;
                }
                flipCountFromPastForCurri++;
                isFlipped[i] = true;
                flips++;
            }
        }

        return flips;
    }
}
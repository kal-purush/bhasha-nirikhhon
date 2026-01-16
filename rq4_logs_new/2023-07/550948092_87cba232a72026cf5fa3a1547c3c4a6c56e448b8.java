package DoIt.TwoPointer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class Solution_2 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int result = 0;
        long[] num = new long[n];
        for(int i = 0; i < 10; i++){
            num[i] = sc.nextInt();
        }
        Arrays.sort(num);
        for(int k = 0; k < n; k++){
            long find = num[k];
            int i = 0;
            int j = n-1;

            while(i < j){
                if(num[i] + num[j] == find){
                    if(i != k && j != k){
                        result++;
                        break;
                    } else if(i == k){
                        i++;
                    } else if(j == k){
                        j--;
                    }
                } else if(num[i] + num[j] < find){
                    i++;
                } else {
                    j--;
                }
            }
        }
        System.out.println(result);

    }
}
package BaekJoon.Greedy;

import java.io.IOException;
import java.util.Scanner;

/*
    2885 초콜릿 식사
    그리디
 */
public class Solution2 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int k = sc.nextInt();
        int len = 1;
        int cnt = 0;

        while(len < k){
            len *= 2;
        }

        int tmp = len;
        while(k > 0){
            if(k < tmp){
                tmp/=2;
                cnt++;
            } else{
                k -= tmp;
            }
        }
        System.out.print(len + " " + cnt);
    }
}
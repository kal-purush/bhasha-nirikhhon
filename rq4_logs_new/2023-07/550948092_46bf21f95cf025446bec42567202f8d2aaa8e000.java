package DoIt.DynamicProgramming;

import java.io.IOException;
import java.util.Scanner;

/*
    086 이친수 구하기
 */
public class Solution3 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        long D[][] = new long[n+1][2];
        D[1][1] = 1; //1로 끝나는
        D[1][0] = 0; //0으로 끝나는
        for(int i = 2; i <= n; i++){
            D[i][0] = D[i-1][1]  + D[i-1][0];
            D[i][1] = D[i-1][0];
        }
        System.out.println(D[n][0] + D[n][1]);
    }
}
package DoIt.DynamicProgramming;

import java.io.IOException;
import java.util.Scanner;

/*
    088 쉬운 계단 수
    점화식
    n일 때의 경우의 수를 n-1일 때의 상황을 고려하여 알아보기
    조건 잘 들여다보기
 */

public class Solution5 {
    static long mod = 1000000000; //출력 조건에 있음 **특별한 조건 놓치지 않도록 유의하기
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        long[][] D = new long[n+1][11];
        for(int i = 1; i <= 9; i++){
            D[1][i] = 1;
        }
        for(int i = 2; i <= n; i++){
            D[i][0] = D[i-1][1];
            D[i][9] = D[i-1][8];
            for(int j = 1; j <= 8; j++){
                D[i][j] = (D[i-1][j-1] + D[i-1][j+1]) % mod;
            }
        }
        long answer = 0;
        for(int i = 0; i <= 9; i++){
            answer = (answer + D[n][i]) % mod;
        }
        System.out.println(answer);
    }
//    static long mod = 1000000000;
//    public static void main(String[] args) {
//        Scanner sc = new Scanner(System.in);
//        int n = sc.nextInt();
//        long D[][] = new long[n+1][11];
//        for(int i = 1; i <= 9; i++){
//            D[1][i] = 1;
//        }
//        for(int i = 2; i <= n; i++){
//            D[i][0] = D[i-1][1];
//            D[i][9] = D[i-1][8];
//            for(int j = 1; j <= 8; j++){
//                D[i][j] = (D[i-1][j-1] + D[i-1][j+1]) % mod;
//            }
//        }
//        int answer = 0;
//        for(int i = 0; i <= 9; i++){
//            answer += D[n][i] % mod;
//        }
//        System.out.println(answer);
//    }
}
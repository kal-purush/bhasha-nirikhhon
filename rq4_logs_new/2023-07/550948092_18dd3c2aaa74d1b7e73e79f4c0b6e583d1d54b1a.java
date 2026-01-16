package DoIt.DynamicProgramming;

import java.io.IOException;
import java.util.Scanner;

/*
    087 2*N 타일 채우기
 */
public class Solution4 {
    static long mod =  10007;
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        long D[] = new long[1001];
        D[1] = 1;
        D[2] = 2;
        for(int i = 3; i <= n; i++){
            D[i] = (D[i-1] + D[i-2]) % mod;
        }
        System.out.println(D[n]);
    }
}
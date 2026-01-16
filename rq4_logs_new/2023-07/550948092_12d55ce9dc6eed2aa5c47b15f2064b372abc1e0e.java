package DoIt.Combination;

import java.io.IOException;
import java.util.Scanner;

/*
    077 이항계수 구하기2
    조합, 모듈러 연산
 */
public class Solution2 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int k = sc.nextInt();
        int[][] D = new int[n+1][n+1];
        for(int i = 0; i <= n; i++){
            D[i][1] = i;
            D[i][0] = 1;
            D[i][i] = 1;
        }

        for(int i = 2; i <= n; i++){
            for(int j = 1; j <= n; j++){
                D[i][j] = D[i-1][j] + D[i-1][j-1];
                D[i][j] = D[i][j] % 10007;
            }
        }
        System.out.println(D[n][k]);
    }
}
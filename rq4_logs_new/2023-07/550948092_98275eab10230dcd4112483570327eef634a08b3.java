package DoIt.Combination;

import java.io.IOException;
import java.util.Scanner;

/*
    076 이항계수 구하기 1
    조합
 */
public class Solution {
    static int n, k;
    static int[][] D;
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        n = sc.nextInt();
        k = sc.nextInt();
        D = new int[n+1][n+1];
        for(int i = 0; i <= n; i++){
            D[i][1] = i;
            D[i][0] = 1;
            D[i][i] = 1;
        }
        for(int i = 2; i <= n; i++){
            for(int j = 1; j <= n; j++){
                D[i][j] = D[i-1][j] + D[i-1][j-1];
            }
        }
        System.out.println(D[n][k]);
    }
}
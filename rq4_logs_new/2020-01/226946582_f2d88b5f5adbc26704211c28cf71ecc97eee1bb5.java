package week05;

import java.util.Scanner;

public class LCS2 {

    private static int lcs2(int[] a, int[] b , int m , int n) {
       int dp [][] = new int[m + 1][n + 1];
       for(int i = 0 ; i <=m ; i++)
       {
           for(int j = 0 ; j <=n ; j++)
           {
               if(i == 0 || j == 0)
               {
                   dp[i][j] = 0;
               }

               else if(a[i - 1] == b[j - 1])
               {
                   dp[i][j] = dp[i-1][j-1] + 1;
               }
               else
               {
                   dp[i][j] = Math.max(dp[i-1][j] , dp[i][j-1]);
               }
           }
       }
       return dp[m][n];
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = scanner.nextInt();
        }

        int m = scanner.nextInt();
        int[] b = new int[m];
        for (int i = 0; i < m; i++) {
            b[i] = scanner.nextInt();
        }

        System.out.println(lcs2(a, b,a.length,b.length));
    }
}
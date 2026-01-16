package DoIt.Combination;

import java.io.IOException;
import java.util.Scanner;

/*
    080 조약돌 꺼내기
 */
public class Solution4 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int m = sc.nextInt(); // 색상 수
        int[] colors = new int[m];
        int n = 0;
        for(int i = 0; i < m; i++) {
            colors[i] = sc.nextInt();
            n += colors[i];
        }
        int k = sc.nextInt();
        int[][] D = new int[n+1][n+1];
        for(int i = 0; i <= n; i++){
            D[i][1] = i;
            D[i][i] = 1;
            D[i][0] = 1;
        }
        for(int i = 2; i <= n; i++){
            for(int j = 1; j <= n; j++){
                D[i][j] = D[i-1][j] + D[i-1][j-1];
            }
        }
        double answer = 0;
        for(int i = 0; i < m; i++){
            answer += D[colors[i]][k];
        }
        answer /= D[n][k];
        System.out.println(answer);
    }
}
package DoIt.Combination;

import java.io.IOException;
import java.util.Scanner;

/*
    079 다리놓기
    경우의 수를 조합으로 구해야 한다는 것을 생각하기 쉽지 않은 문제
 */
public class Solution3 {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        int[][] D = new int[31][31];
        for(int i = 0; i <= 30; i++){
            D[i][1] = i;
            D[i][i] = 1;
            D[i][0] = 1;
        }
        for(int i = 2; i <= 30; i++) {
            for(int j = 1; j <= 30; j++){
                D[i][j] = D[i-1][j] + D[i-1][j-1];
            }
        }
        int q = sc.nextInt(); //질의 수
        for(int i = 0; i < q; i++) {
            int n = sc.nextInt(); //서쪽 사이트 수
            int m = sc.nextInt(); //동쪽 사이트 수
            System.out.println(D[n][m]);
        }
    }
}
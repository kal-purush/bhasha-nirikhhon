package DoIt.Combination;

import java.io.IOException;
import java.util.Scanner;

/*
    082 사전 찾기
    경우의 수를 나열했을 때 k번 째 찾기
    081 순열 순서와 동일한 원리인데 여전히 이해가 어려움
 */
public class Solution6 {
    static int n, m, k;
    static int[][] D;
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        n = sc.nextInt();
        m = sc.nextInt();
        k = sc.nextInt();
        D = new int[202][202];
        for(int i = 0; i <= 200; i++){
            for(int j = 0; j <= i; j++){
                if(j == 0 || j == i)
                    D[i][j] = 1;
                else {
                    D[i][j] = D[i-1][j-1] + D[i-1][j];
                    if(D[i][j] > 1000000000) D[i][j] = 1000000001; //k 범위 넘어가면 범위 최댓값 저장
                }
            }
        }
        if(D[n+m][m] < k) //주어진 자릿수로 만들 수 없는 k번째 수
            System.out.println("-1");
        else{
            while(!(n == 0 && m == 0)){
                //a를 선택했을 때 남은 문자로 만들 수 있는 모든 경우의 수가 k보다 크면
                if(D[n-1+m][m] >= k){
                    System.out.println("a");
                    n--;
                } else {
                    System.out.println("k");
                    k = k - D[n-1+m][m];
                    m--;
                }
            }
        }
    }
}
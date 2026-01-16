package Section02;

import java.util.Scanner;

/**
 * 격자판 최대합
 * 첫번째 N의 숫자를 입력받아 N*N의 격자판을 생성
 * 후에 각 행의 합, 각 열의 합, 각 대각선의 합 중 최대값을 출력한다.
 * */

public class Main09 {

    public int solution(int n , int[][] arr){
        int answer = Integer.MIN_VALUE; //정수중에서 가장 작은 값
        int sum1, sum2;
        for(int i=0;i<n;i++){
            sum1=sum2=0;
            for(int j=0;j<n;j++){
                sum1 += arr[i][j];
                sum2 += arr[j][i];
            }
            answer=Math.max(answer, sum1);
            answer=Math.max(answer, sum2); //sum1이나 sum2 둘 중 하나 큰 값이 들어간다.
        }

        //대각선의 합 --
        sum1=sum2=0;
        for(int i=0;i<n;i++){
            sum1+=arr[i][i]; //대각선의 합
            sum2+=arr[i][n-1-i]; //반대편 대각선의 합
        }
        answer=Math.max(answer, sum1);
        answer=Math.max(answer, sum2);


        return answer;
    }

    public static void main(String[] args) {
        Main09 t = new Main09();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[][] arr = new int[n][n]; //2차원 배열 선언
        for(int i=0;i<n;i++){
            for(int j=0;j<n;j++){
                arr[i][j] = sc.nextInt();
            }
        }
        System.out.println(t.solution(n, arr));

    }
}
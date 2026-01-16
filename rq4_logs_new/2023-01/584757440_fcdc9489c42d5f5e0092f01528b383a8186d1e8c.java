package Section2;

import java.util.Scanner;

/***
 * 소수(에라토스테네스 체) 출력
 * 수업 다시
 */

public class Test05 {

    public int solution(int n){
        int answer=0;
        int[] arr = new int[n];
        for(int i =1;i<=n;i++){
//            if(!(i%2==0 || i%3==0 || i%5==0)) answer++;

        }
        return answer;
    }

    public static void main(String[] args) {
        Test05 t = new Test05();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        System.out.println(t.solution(n));
    }
}
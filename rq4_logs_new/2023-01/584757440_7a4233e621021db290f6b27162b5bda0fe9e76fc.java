package Section02;


import java.util.Scanner;

/**
 * 등수 구하기
 * N명의 학생의 점수가 입력되면 등수를 입려된 순서대로 출력하는 프로그램을 작성
 * 같은 점수가 입력될 경우 높은 등수로 등일 처리 (92점 3명 > 다음 학생은 4등)
 * 등수가 순서대로 출력
 */

public class Main08 {

    public int[] solution(int n, int[] arr){
        int[] answer = new int[n];
        for(int i =0;i<n;i++){
            int cnt = 1;
            for(int j=0;j<n;j++){
                if(arr[j]>arr[i]) cnt++; //j번째가 크면 cnt 등수가 커짐
            }
            answer[i] = cnt;
        }

        return answer;
    }

    public static void main(String[] args) {
        Main08 main = new Main08();
        Scanner kb = new Scanner(System.in);
        int n = kb.nextInt();
        int[] arr = new int[n];
        for(int i =0;i<n;i++){
            arr[i]=kb.nextInt();
        }
        for(int x : main.solution(n, arr)) System.out.print(x + " ");
    }
}
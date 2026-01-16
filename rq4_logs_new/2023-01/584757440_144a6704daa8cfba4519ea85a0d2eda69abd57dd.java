package Section02;

import java.util.Scanner;

public class Main11 {

    public int solution(int n, int[][] arr){
        int answer=0, max=Integer.MIN_VALUE;
        for(int i = 1; i<=n ;i++){
            int cnt = 0;
            for(int j = 1; j<=n; j++){
                for(int k=1; k<=5; k++){ //학년
                    if(arr[i][k]==arr[j][k]) {
                        cnt++;
                        break; //주의
                    }
                }
            }
            if(cnt>max){
                max= cnt;
                answer=i;
            }
        }

        return answer;
    }

    public static void main(String[] args) {
        Main11 t = new Main11();
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[][] arr = new int[n+1][6];  //이차원 배열 동적 생성, 1번부터 시작 n+1, 1학년부터 5학년 6개의 열
        for(int i=0; i<=n; i++){ //학생 번호
            for(int j=0; j<=5; j++){ //1학년부터 5학년까지 번호
                arr[i][j] = sc.nextInt();
            }
        }
        System.out.println(t.solution(n, arr));
    }
}
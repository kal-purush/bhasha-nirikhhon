package ybwi0912;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
 * 2023-09-14
 * BOJ 11729번: 하노이 탑 이동 순서
 * 옮긴 횟수 K, 수행 과정 A B(A번째 탑의 가장 위에 있는 원판을 B번째 탑의 가장 위로 옮긴다) 출력
 * */

public class BOJ11729 {
    static int K; // 옮긴 횟수
    static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        int N = Integer.parseInt(bf.readLine());

        hanoi(N, 1, 3, 2); // 1번 탑에서 2번 탑을 거쳐 3번 탑으로 이동

        System.out.println(K);
        System.out.println(sb);
    }

    static void hanoi(int n, int start, int end, int temp){
        // start번째 탑에서 temp번째 탑을 거쳐 end번째 탑으로 이동. 남은 원판의 수 n
        if(n==1){ // 옮길 원판이 하나만 남았다면
            K++; // 바로 마지막 탑으로 이동, 옮긴 횟수 증가
            sb.append(start + " " + end + "\n");
            return; // 재귀 종료
        }

        hanoi(n-1, start, temp, end); // n-1개를 임시로 사용할 temp에 옮겨놓는다
        sb.append(start + " " + end + "\n"); // 과정 출력
        K++; // 이동 횟수 증가
        hanoi(n-1, temp, end, start); // temp에 옮겼던 원판을 그대로 목적지로 옮긴다
    }
}
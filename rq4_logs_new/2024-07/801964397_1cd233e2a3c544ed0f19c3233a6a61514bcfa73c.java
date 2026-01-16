package no1149;

import java.io.*;
import java.util.*;
public class Main {
    public static void main(String[] args) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine()); //집의 개수
        int[][] cost = new int[n][3];


        for (int i = 0; i < n; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine()," ");
            //각 물감 비용 저장
            cost[i][0]  = Integer.parseInt(st.nextToken()); //R
            cost[i][1]  = Integer.parseInt(st.nextToken()); //G
            cost[i][2]  = Integer.parseInt(st.nextToken()); //B
        }

        for (int i = 1 ; i < n ; i++) {
            //빨강 선택할 경우, 이전에는 빨강을 선택할 수 없으므로 초록, 파랑중에 비교
            cost[i][0] += Math.min(cost[i-1][1], cost[i-1][2]);
            //초록 선택할 경우, 이전에는 초록 선택할 수 없으므로 빨강, 파랑중에 비교
            cost[i][1] += Math.min(cost[i-1][0], cost[i-1][2]);
            //파랑 선택할 경우, 이전에는 파랑 선택할 수 없으므로 빨강, 초록중에 비교
            cost[i][2] += Math.min(cost[i-1][0], cost[i-1][1]);
        }
        System.out.println(Math.min(Math.min(cost[n-1][0], cost[n-1][1]),cost[n-1][2]));
    }
}
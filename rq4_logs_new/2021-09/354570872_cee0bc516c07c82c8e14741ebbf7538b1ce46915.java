package study.Baekjoon.month9_3;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 문제이름 : 해밀턴 순환회로
 * 링크 : http://jungol.co.kr/bbs/board.php?bo_table=pbank&wr_id=954&sca=99&sfl=wr_hit&stx=1681
 */

public class JUNGOL1681 {
    static int N;
    static final int INF = Integer.MAX_VALUE - 100000000; 
    static int[][] cost;
    static int[][] dp;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        N = Integer.parseInt(br.readLine());
        cost = new int[N][N];
        dp = new int[N][1<<N];
        for (int i = 0; i < N; i++) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < N; j++) {
                cost[i][j] = Integer.parseInt(st.nextToken());
            }
        }

        int dist = tsp(0, 1);

        bw.write(dist + "\n");
        bw.close();
    }

    public static int tsp(int curr, int visited) {
        if (visited == (1 << N) - 1) {  //모든 장소를 방문했으면
            if (cost[curr][0] == 0) // 회사로 돌아가는 길이 없으면 INF 반환
                return INF;
            return cost[curr][0];   // 현재장소에서 회사로 돌아가는 코스트 반환
        }
        
        if (dp[curr][visited] != 0) //현재 장소 방문한적 있으면 (가지치기)
            return dp[curr][visited];
        
        int min = INF;
        for (int i = 0; i < N; i++) {
            if (cost[curr][i] != 0 && (visited & (1 << i)) == 0) {  //curr에서 i까지 길이 있고 방문한 적 없으면
                int dist = tsp(i, visited | (1 << i)) + cost[curr][i];
                min = Math.min(min, dist);
            }
        }
        return dp[curr][visited] = min;
    }
}
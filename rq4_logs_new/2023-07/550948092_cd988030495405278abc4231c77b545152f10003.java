package DoIt.BFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

/*
     063 케빈 베이컨의 6단계 법칙
 */
public class Solution5 {
    static int arr[][];
    static boolean visit[];

    static int n;
    static int result;
    static int min = Integer.MAX_VALUE;

    static class Bacon {
        int num;
        int value;

        public Bacon(int num, int value) {
            this.num = num;
            this.value = value;
        }
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n  = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());

        arr = new int[n+1][n+1];
        for(int i = 0; i < m; i++ {
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            arr[s][e] = 1;
            arr[e][s] = 1;
        }
        for(int i = 1; i <= n; i++){
            visit = new boolean[n+1];
            BFS(i);
        }
        System.out.println(result);
    }

    static void BFS(int s){
        Queue<Bacon> queue = new LinkedList<>();
        int cnt = 0;

        visit[s] = true;
        queue.offer(new Bacon(s, 0));
        while(!queue.isEmpty()){
            Bacon bacon = queue.poll();
            cnt += bacon.value;

            for(int i = 1; i <= n; i++){
                int num = arr[bacon.num][i];

                if(num == 1 && visit[i] == false){
                    visit[i] = true;
                    queue.offer(new Bacon(i, bacon.value +1));
                }
            }
        }
        if(min > cnt){
            min = cnt;
            result = s;
        }
    }
}

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
 
public class Solution {
    static int N, answer;
    static int[] weight;
    static boolean[] visited;
    public static void main(String args[]) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
         
        int T = Integer.parseInt(bf.readLine());
        for(int t = 1; t <= T; t++) {
            answer = 0;
            N = Integer.parseInt(bf.readLine());
            weight = new int[N];
            visited = new boolean[N];
            String[] input = bf.readLine().split(" ");
             
            for(int i = 0; i < N; i++) {
                weight[i] = Integer.parseInt(input[i]); 
            }
             
            recur(0, 0, 0);
            System.out.println("#" + t + " " + answer);
        }
    }
     
    static void recur(int left, int right, int depth) {
        if(left < right)
            return;
         
        if(depth == N) {    
            answer += 1;
            return;
        }
         
        for(int i = 0; i < N; i++) {
            if(visited[i] == false) {
                visited[i] = true;
                recur(left + weight[i], right, depth + 1);
                recur(left, right + weight[i], depth + 1);
                visited[i] = false;
            }
        }
    }
}
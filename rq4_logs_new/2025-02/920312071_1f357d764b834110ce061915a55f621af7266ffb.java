import java.util.Arrays;
import java.util.Scanner;

public class Main {
	static int[] num;
	static int[] temp;
	static boolean[] visit;
	static int N, M;
	static StringBuilder sb = new StringBuilder();
	
    public static void main(String[] args) {
    	Scanner sc = new Scanner(System.in);
    	N = sc.nextInt();
    	M = sc.nextInt();
    	num = new int[N];
    	temp = new int[M];
    	visit = new boolean[N];
    	
    	for(int i = 0; i < N; i++) {
    		num[i] = sc.nextInt();
    	}
    	sc.close();
    	
    	Arrays.sort(num);
    	dfs(0);
    	System.out.println(sb);
    }
    
    static void dfs(int depth) {
    	if(depth == M) {
    		for (int i = 0; i < M; i++){
                sb.append(temp[i] + " ");
            }
    		sb.append("\n");
    		return;
    	}
    	for(int i = 0; i < N; i++) {
    		if(!visit[i]) {
    			visit[i] = true;
    			temp[depth] = num[i];
    			dfs(depth + 1);
    			visit[i] = false;
     		}
    	}
    }
}
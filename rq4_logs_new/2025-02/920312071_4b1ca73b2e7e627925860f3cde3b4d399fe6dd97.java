import java.util.Scanner;

public class Main {
	static int N, M;
	static int[] temp;
	static boolean[] visit;
	
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);      
        N = sc.nextInt();
        M = sc.nextInt();
        sc.close();
        
        temp = new int[M];
        visit = new boolean[N];
        
        dfs(0);
    }
    
    static void dfs(int depth) {
    	if(depth == M) {
    		for(int i = 0; i < M; i++) {
    			System.out.print(temp[i] + " ");
    		}
    		System.out.println();
    		return;
    	}
    	
    	for(int i = 0; i < N; i++) {
    		if(visit[i] == false) {
    			visit[i] = true;
    			temp[depth] = i + 1;
    			
    			dfs(depth + 1);
    			
    			visit[i] = false;
    		}
    	}
    }
}
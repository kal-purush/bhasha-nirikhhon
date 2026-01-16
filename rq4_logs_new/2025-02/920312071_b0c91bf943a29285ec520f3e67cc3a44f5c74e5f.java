import java.util.Arrays;
import java.util.Scanner;

public class Main {
	static int dx[] = {0, 0, 1, -1};
	static int dy[] = {1, -1, 0, 0};
	static boolean[][] check;
	static int[][] village;
	static int[] village_cnt;
	static int village_num;
	static int N;
	
    public static void main(String[] args) {
    	Scanner sc = new Scanner(System.in);
    	
    	N = sc.nextInt();
    	village = new int[N][N];
    	check = new boolean[N][N];
    	village_cnt = new int[N * N / 2 + 2];
    	
    	for(int r = 0; r < N; r++) {
    		String input = sc.next();
    		for(int c = 0; c < N; c++) {
    			village[r][c] = input.charAt(c) - '0';
    		}
    	}
    	
    	for(int r = 0; r < N; r++) {
    		for(int c = 0; c < N; c++) {
    			if(village[r][c] == 1 && !check[r][c]) {
    				village_num++;
    				dfs(r, c);
    			}
    		}
    	}
    	
    	System.out.println(village_num);
    	Arrays.sort(village_cnt);
    	
    	for(int i = 0; i < village_cnt.length; i++) {
    		if(village_cnt[i] != 0) 
    			System.out.println(village_cnt[i]);
    	}
    	sc.close();
    }
    
    static void dfs(int x, int y) {
    	check[x][y] = true;
    	village_cnt[village_num] += 1;
    	
    	for(int i = 0; i < 4; i++) {
    		int nx = x + dx[i];
    		int ny = y + dy[i];
    		
    		if(nx >=0 && nx < N && ny >= 0 && ny < N) {
    			if(village[nx][ny] == 1 && !check[nx][ny]) {
    				dfs(nx,ny);
    			}
    		}
    	}
    }
}
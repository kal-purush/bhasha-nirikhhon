import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;

class Main {
	static int W, H, K, move;
	static int[] dx = {0, 0, -1, 1};
	static int[] dy = {1, -1, 0, 0};
	static int[] kx = {-2, -2, -1, -1, 1, 1, 2, 2};
	static int[] ky = {-1, 1, -2, 2, -2, 2, -1, 1};
	static int[][] map;
	static boolean[][][] visited;
	static boolean check;
	
	public static void main(String args[]) throws Exception {
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
		
		K = Integer.parseInt(bf.readLine());
		String[] str = bf.readLine().split(" ");
		W = Integer.parseInt(str[0]);
		H = Integer.parseInt(str[1]);
		map = new int[H][W];
		visited = new boolean[H][W][K + 1];
		
		for(int r = 0; r < H; r++) {
			str = bf.readLine().split(" ");
			for(int c = 0; c < W; c++) {
				map[r][c] = Integer.parseInt(str[c]);
			}
		}
		bfs();
		System.out.println(move);
	}
	
	static void bfs() {
		Queue<int[]> q = new LinkedList<>();
		q.add(new int[] {0, 0, 0});
		
		while(true) {
			check = false;
			int size = q.size();
			for(int i = 0; i < size; i++) {
				int[] temp = q.remove();
				int x = temp[0];
				int y = temp[1];
				int k = temp[2];
				
				if(x == W - 1 && y == H - 1)
					return;
				
				for(int j = 0; j < 4; j++) {
					int nx = x + dx[j];
					int ny = y + dy[j];
					
					if(nx >= 0 && nx < W && ny >= 0 && ny < H && !visited[ny][nx][k] && map[ny][nx] != 1) {
						visited[ny][nx][k] = true;
						q.add(new int[] {nx, ny, k});
						check = true;
					}
				}
				
				if(k < K) {
					for(int j = 0; j < 8; j++) {
						int nx = x + kx[j];
						int ny = y + ky[j];
						
						if(nx >= 0 && nx < W && ny >= 0 && ny < H && !visited[ny][nx][k + 1] && map[ny][nx] != 1) {
							visited[ny][nx][k + 1] = true;
							q.add(new int[] {nx, ny, k + 1});
							check = true;
						}
					}
				}
			}
			if(check == false) {
				move = -1;
				return;
			}
			move += 1;
		}
	}
}
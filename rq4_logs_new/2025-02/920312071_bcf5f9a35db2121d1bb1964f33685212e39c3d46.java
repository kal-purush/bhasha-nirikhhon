import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;

class Main {
	static int R, C;
	static int x, y, nx, ny, sum, day, size, sum_before;
	static int[] dx = {0, 0, -1, 1};
	static int[] dy = {1, -1, 0, 0};
	static Queue<int []> queue = new LinkedList<>();
	static int[][] tomato;
	static int[] now;
	
	public static void main(String args[]) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		String[] str = br.readLine().split(" ");
		C = Integer.parseInt(str[0]);
		R = Integer.parseInt(str[1]);
		tomato = new int[R][C];
		
		for(int r = 0; r < R; r++) {
			str = br.readLine().split(" ");
			for(int c = 0; c < C; c++) {
				tomato[r][c] = Integer.parseInt(str[c]);
				if(tomato[r][c] == 1) {
					queue.add(new int[] {r, c});
					sum += 1;
				}
				if(tomato[r][c] == -1) {
					sum += 1;
				}
			}
		}
		bfs();
		if(sum_before == sum) 
			System.out.println(-1);
		else
			System.out.println(day);
	}
	
	static void bfs() {
		while(sum < R * C && sum_before != sum) {
			size = queue.size();
			sum_before = sum;
			for(int i = 0; i < size; i++) {				
				now = queue.remove();
				x = now[0];
				y = now[1];		
				for(int j = 0; j < 4; j++) {
					nx = x + dx[j];
					ny = y + dy[j];
					if(nx < R && nx >= 0 && ny < C && ny >= 0 && tomato[nx][ny] == 0) {
						tomato[nx][ny] = 1;
						sum += 1;
						queue.add(new int[] {nx, ny});
					}
				}
			}
			day += 1;
		}
	}
}
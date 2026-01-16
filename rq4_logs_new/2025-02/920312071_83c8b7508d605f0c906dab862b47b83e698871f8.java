import java.util.Scanner;

public class Main {
	static int R, C, N;
	static char[][] map;
	static boolean[][] bomb_map1;
	static boolean[][] bomb_map2;
	
	public static void main(String[] args){
		Scanner sc = new Scanner(System.in);
		
		R = sc.nextInt();
		C = sc.nextInt();
		N = sc.nextInt();
		sc.nextLine();
	
		map = new char[R][C];
		bomb_map1 = new boolean[R][C];
		bomb_map2 = new boolean[R][C];
		
		for(int r = 0; r < R; r++) {
			String str = sc.nextLine();
			for(int c = 0; c < C; c++) {
				map[r][c] = str.charAt(c);
			}
		}
		
		for(int r = 0; r < R; r++) {
			for(int c = 0; c < C; c++) {
				if(map[r][c] == 'O')
					bomb(r, c, 1);	
			}
		} 
		
		for(int r = 0; r < R; r++) {
			for(int c = 0; c < C; c++) {
				if(bomb_map1[r][c] == false)
					bomb(r, c, 2);
			}
		}
		
		if(N == 1) {
			for(int r = 0; r < R; r++) {
				for(int c = 0; c < C; c++) {
					if(map[r][c] == 'O') 
						System.out.print('O');
					else
						System.out.print('.');
				}
				System.out.println();
			}
		}
		
		else if(N % 2 == 0) {
			for(int r = 0; r < R; r++) {
				for(int c = 0; c < C; c++) {
					System.out.print('O');
				}
				System.out.println();
			}
		}
		
		else if(N % 4 == 3) {
			for(int r = 0; r < R; r++) {
				for(int c = 0; c < C; c++) {
					if(bomb_map1[r][c] == true) 
						System.out.print('.');
					else
						System.out.print('O');
				}
				System.out.println();
			}
		}
		
		else if(N % 4 == 1) {						
			for(int r = 0; r < R; r++) {
				for(int c = 0; c < C; c++) {
					if(bomb_map2[r][c] == true) 
						System.out.print('.');
					else
						System.out.print('O');
				}
				System.out.println();
			}
		}
	}
	
	static void bomb(int r, int c, int mode) {
		int[] dx = {0, 0, 1, -1};
		int[] dy = {1, -1, 0, 0};
		if(mode == 1)
		bomb_map1[r][c] = true;
		if(mode == 2)
		bomb_map2[r][c] = true;
		
		for(int i = 0; i < 4; i++) {
			int ny = r + dy[i];
			int nx = c + dx[i];
			
			if(ny >= 0 && R > ny && nx >= 0 && C > nx) {
				if(mode == 1)
				bomb_map1[ny][nx] = true;
				if(mode == 2)
				bomb_map2[ny][nx] = true;
			}
		}
	}
}
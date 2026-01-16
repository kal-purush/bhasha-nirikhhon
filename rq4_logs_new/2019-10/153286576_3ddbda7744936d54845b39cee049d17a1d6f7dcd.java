package baekjoon.simulation;


import java.util.*;
import java.io.*;

//Puyo Puyo
public class Q11559 {
	static char[][] map = new char[12][6];
	static List<List<Integer>> boomList = new ArrayList<>();
	static List<Integer> list = new ArrayList<>();
	static boolean[] visited = new boolean[12*6];
	static int[] tx = {1, -1, 0, 0};
	static int[] ty = {0, 0, 1, -1};

	public static void main(String[] args) throws IOException{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		for(int i=0; i<12; i++) {
			String input = br.readLine();
			for(int j=0; j<6; j++) {
				map[i][j] = input.charAt(j);
			}
		}
		int ans = 0;
		while(true) {
			boomList.clear();
			Arrays.fill(visited,false);
			for(int i=0; i<12; i++) {
				for(int j=0; j<6; j++) {
					if(map[i][j] != '.') {
						list.clear();
						bfs(i*6+j, map[i][j]);
						if(list.size()>=4) {
							boomList.add(new ArrayList(list));
						}
					}
				}
			}
			if(boomList.size()==0) break;
			ans++;
			map = boom();
			for(int i=0; i<12; i++) {
				for(int j=0; j<6; j++) {
					System.out.print(map[i][j]);
				}
				System.out.println();
			}
		}
		System.out.println(ans);
	}

	static void bfs(int idx, char c) {
		Queue<Integer> q = new LinkedList<>();
		list.add(idx);
		q.add(idx);
		visited[idx] = true;
		while(!q.isEmpty()) {
			int k = q.poll();
			int x = k/6;
			int y = k%6;
			for(int i=0; i<4; i++) {
				if(x+tx[i]>=0 && x+tx[i]<12 && y+ty[i]>=0 && y+ty[i]<6) {
					if( c == map[x+tx[i]][y+ty[i]]) {
						int next = ((x+tx[i])*6)+y+ty[i];
						if(!visited[next]) {
							visited[next] = true;
							q.add(next);
							list.add(next);
						}
					}
				}
			}
		}
	}

	static char[][] boom() {
		char[][] temp = new char[12][6];
		for(int i=0; i<12; i++) {
			Arrays.fill(temp[i], '.');
		}
		for(List<Integer> k : boomList) {
			for(int j=0; j<k.size(); j++) {
				int x = k.get(j)/6;
				int y = k.get(j)%6;
				map[x][y] = '.';
			}
		}
		
		for(int i=0; i<6; i++) {
			int idx = 11;
			for(int j=11; j>=0; j--) {
				if(map[j][i] != '.') {
					temp[idx--][i] = map[j][i];
				}
			}
		}
		return temp;
	}
}
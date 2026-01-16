package quest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

public class GraphDFS_BFS {

	public static List<ArrayList<Integer>> list = new ArrayList<>();
	public static int n;
	public static boolean [] visited;
	
	public GraphDFS_BFS(int n) {
		
		for(int i=0; i<n; i++)  
			list.add(new ArrayList<>());
			
	}
	
	public void bfs(int v) {
		
		//방문지점 true
		visited[v] = true;
				
		Queue<Integer> q = new LinkedList<>();
		q.offer(v);
		
		while (!q.isEmpty()) {
			
			int w = q.poll();
			System.out.print(w + " ");

			for(int i=0; i<list.get(w).size(); i++) {
				
				int x = list.get(w).get(i);
				
				if(!visited[x]) {
					q.offer(x);
					visited[x] = true;
				}
			}
		}
	
	}
	
	public void dfs(int v) {
		
		
		visited[v] = true;
		System.out.print(v + " ");
		
        for(int i=0; i<list.get(v).size(); i++){

            int w = list.get(v).get(i);

            if(!visited[w]) {
                dfs(w);
            }
        }
	}

	
	public void insertVertex(int start, int end) {
		
		if(start == end)
			return;
		
		//양방향 연결
		//중복데이터 삽입 안합
		if(list.get(start).size()==0)	
			list.get(start).add(end);
		else
			if(!list.get(start).contains(end)) 
				list.get(start).add(end);
		
		if(list.get(end).size()==0)
			list.get(end).add(start);
		else
			if(!list.get(end).contains(start))
				list.get(end).add(start);
	}
	
	public void displayAdjacencyLists() {
		
		for(int i=0; i<n; i++) {
			
			Collections.sort(list.get(i));
			System.out.println("start node " + i + "==>" + list.get(i));
			// new TreeSet<>(list.get(i))
		}
	}
	
	
	public static void main(String[] args) {
		
		Scanner sc = new Scanner(System.in);
		System.out.print("사이즈 n 입력: ");
		n = sc.nextInt();
		System.out.println();
		
		GraphDFS_BFS g = new GraphDFS_BFS(n);
		
		int select = -1;
		int startEdge = -1;
		int endEdge = -1;
		
		while(select != 0) {
			
			System.out.println("\nSelect command 1: Add edges, 2: Display Adjacency Lists, "
					+ "3: BFS, 4: DFS, 5: Quit => ");
			
			select = sc.nextInt();
			
			switch(select) {
			
				case 1:
					System.out.println("Add edges");
					System.out.print("input start node : ");
					startEdge = sc.nextInt();
					System.out.println("input end node : ");
					endEdge = sc.nextInt();
					
					if(startEdge<0 || startEdge>=n || endEdge<0 || endEdge>=n) {
						System.out.println("the input node is out of bound.");
						break;
					}
					
					g.insertVertex(startEdge, endEdge);
					break;
					
				case 2:
					g.displayAdjacencyLists();
					break;
			
				case 3:
					System.out.println("BFS");
					visited = new boolean [n];
					System.out.print("Start BFS from node: ");
					startEdge = sc.nextInt();
					g.bfs(startEdge);
					break;
				case 4:
					System.out.println("DFS");
					visited = new boolean [n];
					System.out.print("Start DFS from node: ");
					startEdge = sc.nextInt();
					g.dfs(startEdge);
					break;
				case 5:
					System.out.println("프로그램 종료");
					select = 0;
					break;
				default:
					System.out.println("Wrong input, re-enter");
					break;
					
			}
			
		}
		
		System.out.println("pause");
		
	}

}
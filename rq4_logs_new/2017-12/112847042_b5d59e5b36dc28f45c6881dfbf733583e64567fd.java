import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

//Define a class Graph containing three attributes
public class Graph {
	private final int N; // the order of the graph (number of nodes)
	private int M; // the size of the graph (the number of edges)
	private List<Integer>[] adj;
	private Map<Integer, List<Integer>> Adjacency_List;// //an adjacency list
														// representation of the
														// graph

	// Initializes a graph from a specified input stream and reading the graph
	// from the graph.txt file
	public Graph(String filePath) throws IOException {
		
		List<String> lines = Files.readAllLines(Paths.get(filePath),
				StandardCharsets.UTF_8);
		Set<String> set_1 = new HashSet<String>();// get order from txt
		for (String line : lines) {
			String[] nodesNumber = line.split(" ");
			for (String nodeNumber : nodesNumber) {
				set_1.add(nodeNumber);
			}
		}
		N=set_1.size();
		Set<String> set_2 = new HashSet<String>();// get size from txt
		for (String line : lines) {
			String[] ordersNumber = line.split("\n");
			for (String orderNumber : ordersNumber) {
				set_2.add(orderNumber);
			}
		}
		M=set_2.size();
		
		adj = new ArrayList[N];

		

		Adjacency_List = new HashMap<Integer, List<Integer>>();
		for (int i = 1; i <= N; i++) {
			Adjacency_List.put(i, new LinkedList<Integer>());
		}
		addEdgesToGraph(lines);
	}

	// keyboard input
	public Graph(int N, int M) throws IOException {
		this.N = N;
		this.M = M;
		Adjacency_List = new HashMap<Integer, List<Integer>>();
		for (int i = 1; i <= N; i++) {
			Adjacency_List.put(i, new LinkedList<Integer>());
		}
	}

    private void addEdgesToGraph(List<String> lines) {

		for (String line : lines) {
			String[] nodesNumber = line.split(" ");
			int node1Index = Integer.parseInt(nodesNumber[0]);// string to int
			int node2Index = Integer.parseInt(nodesNumber[1]);
			addEdge(node1Index, node2Index);

		}
	}

	public void addEdge(int u, int v) {

		LinkedList<Integer> start_list = (LinkedList<Integer>) Adjacency_List.get(u);
		//for (int i = 0; i <= N; i++) {
			if (!start_list.contains(v)) {
				start_list.add(v);
			//}
			
			
		}//System.out.println(u +"->"+start_list);
		LinkedList<Integer> end_list = (LinkedList<Integer>) Adjacency_List.get(v);
		//for (int i = 0; i <= N; i++) {
			if (!end_list.contains(u)) {
				end_list.add(u);
			}
		//}

	}

	// max , min and average DEGREE
	public List<Integer> Neighbors(int v) {
		int MAX = Adjacency_List.get(1).size();
		int MIN = Adjacency_List.get(1).size();
		int SUM = 0;
		
		

		for (int i = 1; i < N; i++) {
			MAX = Math.max(MAX,
					Adjacency_List.get(i+1).size());
			MIN = Math.min(MIN,
					Adjacency_List.get(i+1).size());
			SUM = SUM + Adjacency_List.get(i).size();
		}
		System.out.println("THE MAX DEGREE IS: " + MAX);
		System.out.println("THE MIN DEGREE IS: " + MIN);
		System.out.println("THE AVERAGE DEGREE IS: " + SUM / N);
		return Adjacency_List.get(v);
	}

	public void print() {
		
		System.out.println("Adjacency List of the graph  is :");
		for (int i = 1; i <= N; i++) {
			System.out.print(i);
			System.out.print("-->");
			for (int j = 1;; j++) {

				if (j != Adjacency_List.get(i).size()) {
					System.out.print(Adjacency_List.get(i).get(j-1) + "-->");
				} else {
					System.out.print(Adjacency_List.get(i).get(j-1));
					break;
				}

			}
			System.out.println();
		}
		System.out.println("THE NUMBER OF NODES IS : " + N);
		System.out.println("THE NUMBER OF EDGES IS : " + M);
	   Neighbors(N);
	}
	
}
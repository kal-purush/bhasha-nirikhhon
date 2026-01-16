package ds.graph;

import java.util.LinkedList;

public class Graph1 {

	static class Graph{
		int d;
		LinkedList<Integer> g[];
		
		public Graph(int d) {
			// TODO Auto-generated constructor stub
			this.d = d;
			g= new LinkedList[d];
			for(int i = 0; i < d ; i++){
				g[i]= new LinkedList<>();
			}
		}
	}
	
	/***
	 * graph implementation by LinkedList
	 * @param args
	 */
	public static void main(String[] args) {
		int v=5;
		Graph g = new Graph(v);
		addEdge(g, 0, 1);
        addEdge(g, 0, 4);
        addEdge(g, 1, 2);
        addEdge(g, 1, 3);
        addEdge(g, 1, 4);
        addEdge(g, 2, 3);
        addEdge(g, 3, 4);
        
        printGraph(g);
	}
	
    static void printGraph(Graph graph)
    {       
        for(int v = 0; v < graph.d; v++)
        {
            System.out.println("Adjacency list of vertex "+ v);
            System.out.print("head");
            for(Integer pCrawl: graph.g[v]){
                System.out.print(" -> "+pCrawl);
            }
            System.out.println("\n");
        }
    }
	
	private static void addEdge(Graph gf, int x, int y){
		 gf.g[x].addFirst(y);
		 gf.g[y].addFirst(x);
	}
}
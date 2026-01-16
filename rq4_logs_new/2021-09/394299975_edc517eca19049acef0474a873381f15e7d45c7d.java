package topic.graphs;
import java.util.*;
public class GraphsOperation {
	
	/**
	 * to add edges to the graph
	 * @param adj
	 * @param u
	 * @param v
	 * @return
	 */
	static ArrayList<ArrayList<Integer>> addEdge(ArrayList<ArrayList<Integer>> adj, int u, int v){ 
		adj.get(u).add(v); 
		adj.get(v).add(u); 
		return adj;
	} 

}
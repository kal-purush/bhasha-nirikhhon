import java.util.*;
import java.io.File;

public class GraphProcessor {
	
	public GraphProcessor(String graphData) { 
		
	}
	
	/**
	 * Return the out degree of vertex v.
	 * @param v
	 * @return out degree of vertex v.
	 */
	public int outDegree(String v) { 
		
		return 0;
	}
	
	/**
	 * Calculate the BFS path from vertex u to vertex v.
	 * @param u, start vertex
	 * @param v, finish vertex
	 * @return ArrayList of strings repr bfs path from vertex u to vertex v.
	 */
	public ArrayList<String> bfsPath(String u, String v) { 
		
		return null; 
	}
	
	/**
	 * For strongly connected graphs, the diameter is the smallest number, d, 
	 * such that there is a path of length <= d between any pair of 
	 * vertices. 
	 * 
	 * For graphs that are not strongly connected
	 * the diameter is 2n. 
	 * @return diameter of the graph. 
	 */
	public int diameter() { 
		return 0; 
	}
	
	/**
	 * Given a vertex x in V, the centrality of x is the number 
	 * of shortest paths that go via x. 
	 * 
	 * {<x, y> | x, y in V, at least one shortest path from x to y via v}
	 * @param v
	 * @return
	 */
	public int centrality(String v) { 
		
		return 0;
	}
}
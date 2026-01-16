import structures.Vertex ;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import structures.Graph;

/*
 * 	This is a slight modification of BFS as needed for this assignment
 * 	Instead of using WHITE, GRAY and BLACK as coloring, we use RED and BLUE as coloring
 *  	and "" if a node is unvisited
 *  
 */
public class BFSEngine {
	//Graph object representing the graph
	public Graph graph;
	
	//If graph is not 2-Colorable, the violating cycle is stored here
	public List<Integer> answer;
	
	//Initializes the engine
	public BFSEngine(Graph graph){
		this.graph = graph;
	}
	
	//Step one of the BFS
	public void BFS(){
		//For every vertex in the graph
		for(Vertex u: graph.vertices){
			//If it is unvisited
			if(u != null && u.rb.equals("")){
				//Initialize the color as red
				u.rb = "RED";
				//Call BFS Visit
				this.BFS_VISIT(u);
			}
		}
	}
	public void BFS_VISIT(Vertex u){
		//Initializes the queue
		LinkedList<Vertex> queue = new LinkedList<Vertex>();
		queue.add(u);
		while(!queue.isEmpty()){
			Vertex curr = queue.poll();
			for(Vertex v: curr.adjVerts){
				
				//If an adjacent vertex is unvisited
				if(v != null && v.rb.equals("")){
					
					//Set the parent of the adjacent vertex to the current vertex
					v.parent = curr;
					
					//Sets the color of the adjacent vertex to color opposite to that of current vertex
					if(curr.rb.equals("RED")){
						v.rb = "BLUE";
					}
					else if(curr.rb.equals("BLUE")){
						v.rb = "RED";
					}
					queue.add(v);
				}
				/*
				 * 	If an adjacent vertex is visited and that vertex has a color equal to that of the current vertex
				 * 	If this conditional is ever satisfied, the current graph is not 2-Colorable
				 */
				else if(v != null && v.rb.equals(curr.rb)){
					//Checks if we're going in circles or not
					if(curr.parent.root != v.root){
						//Backtracks from the current node and will eventually reach node 1 or the other node with the same color
						List<Integer> backList = this.backTrack(curr, v);
						
						//Backtracks from the violating node until node 1 or the current adjacent node
						List<Integer> backList2 = this.backTrack(v, curr);
						
						/*
						 * 	If neither of the lists resulting from the backtracking is null
						 * 	passes the lists to setupAnswer to find the odd cycle
						 * 
						 */
						if(backList != null && backList2 != null){
							answer = this.setupAnswer(backList, backList2);
						}
					}
				}
			}
		}
	}
	
	/*
	 * 	Given a root node and a target vertex
	 * 	travels backwards through the parent of the current node until it reaches vertex 1 or the target vertex
	 * 	Returns a list of Integers denoting the path this backtracking took
	 */	
	public List<Integer> backTrack(Vertex curr, Vertex target){
		List<Integer> back = new LinkedList<Integer>();
		while(curr != null){
			back.add(curr.root);
			if(curr.root == target.root){
				break;
			}
			curr = curr.parent;
		}
		return back;
	}
	
	/*
	 * 	Takes in two lists that represent paths resulting from backtracking as parameters
	 * 	Suppose that we have two paths in the form of Integer lists
	 * 		A: 8 -> 7 -> 6 -> 5 -> 4 -> 3 -> 2 -> 1
	 * 		B: 14 -> 13 -> 12 -> 11 -> 10 -> 4 -> 3 -> 2 -> 2
	 * 	The path we need to return consists of the elements they do not share AND
	 * 	the first element they have in common so this method finds:
	 * 		8 -> 7 -> 6 -> 5 -> 4 -> 14 -> 13 -> 12 -> 11 -> 10
	 */
	public List<Integer> setupAnswer(List<Integer> backList, List<Integer> backList2){
		
		//List representing the final cycle
		List<Integer> finalList = new ArrayList<Integer>();
		
		//Loop through the first path
		for(int i = 0; i < backList.size(); i++){
			//Current element
			int ele = backList.get(i);
			
			//Conditional will first trigger upon first instance of common element
			if(backList2.contains(ele)){
				
				//Adds that first common element to the final cycle
				finalList.add(ele);
				
				//Adds the non-common elements in reversed order to guarantee that it's a cycle
				for(int j = backList2.size() - 1; j >= 0; j--){
					int curr2 = backList2.get(j);
					if(!backList.contains(curr2)){
						finalList.add(curr2);
					}
				}
				return finalList;
			}
			finalList.add(ele);
		}
		return null;
	}

	//Sends away the odd cycle
	public List<Integer> getAnswer(){
		return answer;
	}
}
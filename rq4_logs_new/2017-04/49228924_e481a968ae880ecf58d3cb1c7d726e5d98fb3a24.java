import java.util.LinkedList;
import java.util.Queue;

public class BFS {
	
	public class Vertex {
		char label;
		boolean vistited;
	}
	
	int adjMatrix[][];
	Vertex vertices[];
	int vertextCount = 0;
	
	Queue queue = new LinkedList<Integer>();	
	
	 public int[][] addEdge(int start, int end){
		adjMatrix[start][end] = 1;
		adjMatrix[end][start] = 1;
		return adjMatrix;
	}
	 
	 public void addVertex(char label){
		 Vertex vertex = new Vertex();
		 vertex.label = label;
		 vertex.vistited = false;
		 vertices[vertextCount++] = vertex;
	 }
	 
	 public int displayVertex(int vertexIndex){
		return vertices[vertexIndex].label;
	 }
	 
	 
	 public int getAdjUnvisitedVertex(int vertexIndex){
		 for(int i =0; i < vertexIndex; i++){
			 if(adjMatrix[vertexIndex][i] == 1 && vertices[i].vistited == false){
				 return i;
			 }
		 }
		 return -1;
	 }
	 
	 public void bfs(){
		 int i; 
		 vertices[0].vistited = true;
		 displayVertex(0); 
		 queue.add(0);
		 
		 int univistedVertex;
		 
		 while(!queue.isEmpty()){
			 int tempVertex = (int) queue.remove();
			 while((univistedVertex = getAdjUnvisitedVertex(tempVertex)) != -1){
				 vertices[univistedVertex].vistited = true;
				 
			 }
		 }
	 }

	
	public void main(String[] args) {
		
		for(int i =0; i< queue.size(); i++){
			for(int j =0; j < queue.size(); j++){
				adjMatrix[i][j] = 0;
			}
		}
		addVertex('A');
		addVertex('B');
		addVertex('C');
		addVertex('D');
		addEdge(0,1);
		addEdge(0,2);
		addEdge(0,3);
		addEdge(1,4);
		addEdge(2,4);
		addEdge(3,4);
		bfs();
	}
}
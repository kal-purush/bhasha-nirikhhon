package aiProjectFinal;
import java.util.ArrayList;

public class Main {
	public static void main(String asad[]) {
		
//		Graph g = new Graph(5);
//		g.addVertex("Xi");
//		g.addVertex("Ch");
//		g.addVertex("Be");
//		g.addVertex("Hg");
//		g.addVertex("Ha");
//		
//		g.addEdge("Xi", "Ch", 606);
//		g.addEdge("Xi", "Hg", 1150);
//		g.addEdge("Xi", "Be", 914);
//		g.addEdge("Ch", "Hg", 1539);
//		g.addEdge("Ch", "Be", 1518);
//		g.addEdge("Be", "Hg", 1134);
//		g.addEdge("Be", "Ha", 1061);
//		g.addEdge("Ha", "Hg", 1822);
//		
//		g.setStartLocation("Hg");
		
		Graph g = new Graph(10);
		g.addVertex("A");
		g.addVertex("B");
		g.addVertex("C");
		g.addVertex("D");
		g.addVertex("E");
		g.addVertex("F");
		g.addVertex("G");
		g.addVertex("H");
		g.addVertex("I");
		g.addVertex("J");
		
		g.addEdge("A", "B", 4);
		g.addEdge("A", "C", 3);
		g.addEdge("A", "G", 4);
		g.addEdge("B", "D", 2);
		g.addEdge("B", "E", 3);
		g.addEdge("C", "E", 1);
		g.addEdge("C", "G", 5);
		g.addEdge("D", "E", 3);
		g.addEdge("D", "J", 10);
		g.addEdge("E", "F", 3);
		g.addEdge("E", "I", 3);
		g.addEdge("F", "G", 2);
		g.addEdge("F", "H", 5);
		g.addEdge("G", "H", 6);
		g.addEdge("H", "I", 1);
		g.addEdge("I", "J", 1);
		
		g.setStartLocation("A");
	}
}

class Graph {

	private int vertexCount;
	private ArrayList<String> vertexList;
	private ArrayList<String> visitedList;
	private int[][] adjacencyMatrix;
	private int[][] stack;
	private int stackCount;
	private double start;
	private double stop;
	private String startLocation;
	private String temp = null;

	public Graph(int vertex) {
		vertexCount = 0;
		vertexList = new ArrayList<String>();
		visitedList = new ArrayList<String>();
		adjacencyMatrix = new int[vertex][vertex];
		stack = new int[vertex][3];
		stackCount = 0;
	}

	public void addVertex(String v) {
		if (!vertexList.contains(v)) {
			vertexList.add(v);
			vertexCount++;
		}
	}

	public void addEdge(String from, String to, int value) {
		adjacencyMatrix[vertexList.indexOf(from)][vertexList.indexOf(to)] = value;
		adjacencyMatrix[vertexList.indexOf(to)][vertexList.indexOf(from)] = value;

	}

	public void setStartLocation(String loc){
		startLocation = loc;
		start = System.nanoTime();
		bestFirstSearch(startLocation);
	}

	private boolean isEmpty() {
		return stackCount == 0;
	}

	private boolean isFull() {
		return stackCount == stack.length;
	}

	private void stackSort() {
		int temp1, temp2, temp3;
		for (int i = 0; i < stackCount - 1; i++) {
			for (int j = 0; j < stackCount - i - 1; j++) {
				if (stack[j + 1][2] >= stack[j][2]) {
					temp1 = stack[j][0];
					temp2 = stack[j][1];
					temp3 = stack[j][2];

					stack[j][0] = stack[j + 1][0];
					stack[j][1] = stack[j + 1][1];
					stack[j][2] = stack[j + 1][2];

					stack[j + 1][0] = temp1;
					stack[j + 1][1] = temp2;
					stack[j + 1][2] = temp3;
				}
			}
		}
	}
	
	private void emptyStack(){
		for(int a=0;a<=stackCount;a++){
			stack[a][0]=0;
			stack[a][1]=0;
			stack[a][2]=0;
		}
		stackCount = 0;
	}

	private void push(int rowIndex, int columnIndex, int value) {
		if (isFull()) {
			int[][]stack1=new int[stack.length+stack.length][3];
			for(int i=0;i<stack.length;i++){
				for(int j=0;j<3;j++){
					stack1[i][j]=stack[i][j];
				}
			}				
			stack=stack1;
		}
		stack[stackCount][0] = rowIndex;
		stack[stackCount][1] = columnIndex;
		stack[stackCount][2] = value;
		stackCount++;
	}

	private int pop() {
		if (!isEmpty()) {
			stackCount--;
			int a = stack[stackCount][1];
			return a;
		} else {
			System.out.println("Stack Empty");
			return -99;
		}
	}

	private void bestFirstSearch(String from) {
		int row = vertexList.indexOf(from);
		for (int i = 0; i < vertexCount; i++) {
			if (adjacencyMatrix[row][i] > 0) {
				push(row, i, adjacencyMatrix[row][i]);
			}
		}
		if(!visitedList.contains(from)){
			visitedList.add(from);
			temp = from;
		}
		stackSort();
		getNext();
	}

	private void getNext() {
		if (!isEmpty()) {
			int nextRow = pop();
			String val = vertexList.get(nextRow);
			if(temp != null){
				destroyReversePath(val,temp);
				temp = null;
			}
			if (val==startLocation || !visitedList.contains(val)) {
				if(val == startLocation && visitedList.size() == vertexList.size()){
					visitedList.add(val);
					display();
				}else{
					emptyStack();
					bestFirstSearch(val);
				}				
			} else {
				getNext();
			}
		} else {
			display();
		}
	}
	
	private void destroyReversePath(String from,String to){
		int fromIndex = vertexList.indexOf(from);
		int toIndex = vertexList.indexOf(to);
		adjacencyMatrix[fromIndex][toIndex] = 0;
	}
	
	private void display() {
		stop = System.nanoTime();
		for (int i = 0; i < visitedList.size(); i++) {
			System.out.print(visitedList.get(i) + "-->");
		}
		visitedList.clear();
		System.out.println();
		double diff = stop - start;
		diff = diff / 1000;
		System.out.println(diff + "us");
	}
}

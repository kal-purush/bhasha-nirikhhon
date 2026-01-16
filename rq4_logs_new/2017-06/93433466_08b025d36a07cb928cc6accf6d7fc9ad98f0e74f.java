import java.util.*;
class Graph{
	
	private ArrayList<Node> nodeList;

	public Graph(){
		nodeList = new ArrayList<Node>();
	}

	public void addNode(Node node){
		nodeList.add(node);
	}
	public ArrayList<Node> getNodeList(){
		return nodeList;
	}
	private int checkMinimum(ArrayList<Integer> cost){
		int minimumValue = 0;// index minimum value
		for(int i = 1; i < cost.size(); i++){
			if( cost.get(i) < cost.get(minimumValue) ){
				minimumValue = i;
			}
		}
		return minimumValue;
	}

    // @Test
	public void checkMinimumTest(){
		ArrayList<Integer> numbers = new ArrayList<Integer>();
		numbers.add(1);
		numbers.add(002);
		numbers.add(300);
		numbers.add(-9);
		numbers.add(3);
		numbers.add(5);

		int minimumIndex = checkMinimum(numbers);
		if(minimumIndex == 3){
			System.out.printf(" true");
		}else{
			 System.out.printf("false");
		}
	}
	// endTest


	public void prim(){
		if(nodeList.isEmpty()){return;}
		ArrayList<Integer> cost = new ArrayList<Integer>();
		ArrayList<Node> originNode = new ArrayList<Node>();
		ArrayList<Node> endNode = new ArrayList<Node>();

		Node firstNode = nodeList.get(0);
		firstNode.markVisited();
		for(Map.Entry<Integer, Node> node: firstNode.getNeighbors().entrySet()){
			originNode.add(firstNode);
			cost.add(node.getKey());
			endNode.add(node.getValue());
		}

		while(!cost.isEmpty()){
			int minimumIndex = checkMinimum(cost);
			Node minimumCostEndNode = endNode.get(minimumIndex);

			if(!minimumCostEndNode.hasBeenVisited()){
				Node minimumCostOriginNode = originNode.get(minimumIndex);
				System.out.printf("%d ->%d ",minimumCostOriginNode.getValue(),minimumCostEndNode.getValue());
				minimumCostOriginNode.markVisited();
				minimumCostEndNode.markVisited();
				for(Map.Entry<Integer, Node> node: minimumCostEndNode.getNeighbors().entrySet()){
					if(!node.getValue().hasBeenVisited()){
						originNode.add(minimumCostEndNode);
						cost.add(node.getKey());
						endNode.add(node.getValue());
					}
				}
			}else{
				cost.remove(minimumIndex);
				originNode.remove(minimumIndex);
				endNode.remove(minimumIndex);
			}
		}
		System.out.println(" ");
	}
	public void getReversedGraph(){
    //creates a new list of nodes that has the same values as the original one
    ArrayList<Node> reversedGraph = new ArrayList<Node>();
    for (Node node: this.getNodeList() ){
        Node newNode = new Node(node.getValue());
        reversedGraph.add(newNode);
    }

    //running through the list of adjacency list backwards
    for (int aux=(nodeList.size()-1); aux>=0; aux--){
        Node currentNode = nodeList.get(aux);
        //running through the adjacency list
        for(int aux2= 0; aux2<currentNode.getNeighbors().size(); aux2++){
            //here we get a Node from the reversedGraph and place it as
            //a neighbor in the adjacency list of the reverseGraph
            for ( Map.Entry<Integer, Node> entry : currentNode.getNeighbors().entrySet()) {
                Node newNode = new Node(0);
                newNode = reversedGraph.get(aux2);
                Integer newWeight = entry.getKey();
                reversedGraph.get(aux2).addNeighbor(newNode, newWeight);

            }
        }
    }
    //print the reverse graph using a search
    System.out.println("Busca em profundidade no grafo reverso:");
    reversedGraph.get(0).buscaProfundidadeByGraph(reversedGraph);
  }

}
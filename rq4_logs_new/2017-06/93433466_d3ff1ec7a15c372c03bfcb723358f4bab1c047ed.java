class CreateG{

	public static void associateAsNeighbors(Node firstNode, Node secondNode, int weight){ // for bi directional graphs
		firstNode.addNeighbor(secondNode,weight);
		secondNode.addNeighbor(firstNode,weight);
	}

	public static void main(String[] args){
		Node firstNode = new Node(5);
		Node secondNode = new Node(7);
		associateAsNeighbors(firstNode,secondNode,3);
		Node thirdNode = new Node(10);
		associateAsNeighbors(secondNode,thirdNode,7);
		associateAsNeighbors(thirdNode,firstNode,10);
		Node fourthNode = new Node(15);
		associateAsNeighbors(fourthNode,firstNode,5);
		firstNode.showNeighbors();
		secondNode.showNeighbors();
		thirdNode.showNeighbors();
		fourthNode.showNeighbors();
		System.out.println("Busca em Profundidade for secondNode");
		//secondNode.bFS();
		//secondNode.buscaProfundidade();
		fourthNode.buscaProfundidade();

	}

}
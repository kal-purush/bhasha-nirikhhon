import java.util.LinkedList;
import java.util.Queue;

public class BFS_level_order_travesal {
	
	public static class Node{
		
		int data;
		Node left;
		Node right;
		Node(int data)
		{
			this.data=data;
		}
	}
		
	public static void main(String[] args) {
		Node root = createBinaryTree();
		BFS(root);
	}
	
	public static void BFS(Node startNode){
				
	// Queue is an interface. You can't instantiate an interface directly except via an anonymous inner class
		
		Queue<Node> queue = new LinkedList<Node>();
		queue.add(startNode);
		
		while(!queue.isEmpty()){
			
			Node temp= queue.poll();
			
			System.out.println(temp.data);
			
			if(temp.left!=null)
				queue.add(temp.left);
			if(temp.right!=null)
				queue.add(temp.right);
		}
		
	}
	
	public static Node createBinaryTree(){
		
		Node rootNode = new Node(40);
		
		Node node30   = new Node(30);
		
		Node node20 = new Node(20); 
		
		Node node10   = new Node(10);
		
		Node node50 = new Node(50); 
		
		Node node60   = new Node(60);
			
		Node node70 = new Node(70); 
		
			
		rootNode.left = node20;
		
		rootNode.right = node60;
		
		node60.left = node50;
		
		node60.right = node70;
		
		node20.left = node10;
		
		node20.right = node30;

		return rootNode;
	}
}
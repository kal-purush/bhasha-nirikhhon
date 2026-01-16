class Node{
	Node left,right;
	int data;
	public Node(int data){
		this.data = data;
	}
}

public class BST{

	public static Node root;
	// public BST(){
		// this.root = root;
	// }
	public void traverse(Node tmpNode){
		if(tmpNode!=null){
			traverse(tmpNode.left);
			System.out.print(tmpNode.data+" ");
			traverse(tmpNode.right);
		}
	}
	public void insert(int data){
		Node newNode = new Node(data);
		if(root==null){
			root = newNode;
			return;
		}
		Node current = root;
		Node parent  = root;

		while(true){
			parent = current;
			if(data > current.data){
				current = current.right;
				if(current==null){
					parent.right = newNode;
					return;
				}
			}else{
				current = current.left;
				if(current==null){
					parent.left = newNode;
					return;
				}
			}
		}
	}
	public boolean find(int data){
		Node current = root;
		while(current.data!=data){
			if(data > current.data){
				current = current.right;
			}else{
				current = current.left;
			}

			if(current==null){
				return false;
			}
		}
		return true;
	}
	public void show(Node root){
		traverse(root);
		System.out.println();
	}
	public boolean delete(int data){
		//initialize
		Node parent = root;
		Node current = root;
		boolean isLeft = false;
		while(current.data!=data){
			parent = current;
			if(data>current.data){
				current = current.right;
				isLeft = false;
			}else{
				current = current.left;
				isLeft = true;
			}

			if(current==null){
				return false;
			}
		}

		//case 1 , the node i want to delete have no childs
		if(current.left==null&&current.right==null){
			if(current==root){
				root=null;
			}

			if(isLeft){
				parent.left = null;
			}else{
				parent.right = null;
			}
		}
		//case 2 , the node i want to delete have one childs
		else if(current.left==null){
			//right child has data
			if(current==root){
				root = current.right;
			}else if(isLeft){
				parent.left = current.right; // because left child is null
			}else{
				parent.right = current.left;
			}

		}else if(current.right==null){
			if(current==root){
				root = current.left;
			}else if(isLeft){
				parent.left = current.right; // because left child is null
			}else{
				parent.right = parent.left;
			}
		}
		//case 3 , the node i want to delete have two childs 
		else if(current.right!=null&&current.left!=null){
			//   chose minimun in right sub tree
			//or chose maximun in left sub tree
			//we chose one
			Node successor = getSuccessor(current);
			if(current==root){
				root = successor;
			}else if(isLeft){
				parent.left = successor;
			}else{
				parent.right = successor;
			}	
			successor.left = current.left;
		}
		return true;
	}
	public Node getSuccessor(Node deleteNode){
		Node successor = null;
		Node successorParent = null;
		Node current = deleteNode.right;
		while(current!=null){
			successorParent = successor;
			successor = current;
			current = current.left;
		}
		if(successor!=deleteNode.right){
			successorParent.left =  successor.right;
			successor.right = deleteNode.right;
		}
		return successor;
	}
	public static void main(String[] args){
		BST tree = new BST();
		tree.insert(5);tree.insert(6);tree.insert(8);
		tree.insert(1);tree.insert(6);tree.insert(3);
		System.out.println("original tree:");
		tree.show(tree.root);
		System.out.println("find node 5 is : " + tree.find(50));
		System.out.println("find node 5 is : " + tree.find(5));
		System.out.println("delete node 5:");
		tree.delete(5);
		tree.show(tree.root);
	}
}
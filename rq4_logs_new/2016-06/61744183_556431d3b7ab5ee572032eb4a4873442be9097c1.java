
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

//define two colors, red and black
enum Color{
	red,black;
}
//define a class including all properties of red black tree and set isNil()function to be false
class RB_Node {
	int ID;
	int count;
	RB_Node left, right, parent;
    Color color;
	public RB_Node(int ID, int count, RB_Node left, RB_Node right,
			RB_Node parent, Color color) {
		
		this.ID = ID;
		this.count = count;
		this.left = left;
		this.right = right;
		this.parent = parent;
		this.color = color;
	}
	public RB_Node(){
		
	}
	public boolean isNil(){
		return false;
	}
}
/*define a class indicating nil nodes 
which inheritate all properties of internal nodes but set isNill() function to true*/
class  Nil extends RB_Node{
	public Nil() {
		super();
		this.color = Color.black;
		// TODO Auto-generated constructor stub
	}
	public boolean isNil(){
		return true;
	}	
}
//read input file and put all nodes into an arraylist, initially all nodes are colored black;
public class RB_tree {
	RB_Node root ;
	public  void createTree(String filename){
		ArrayList<RB_Node> RB_input = new ArrayList<RB_Node>();
		Nil nil = new Nil();
		int numOfNodes = 0;
		try {
			FileInputStream fis = new FileInputStream(filename);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
		    numOfNodes = Integer.valueOf(line);
			int i = 1;
			while ((line = br.readLine()) != null && i <= numOfNodes) {
                String[] s= line.split(" ");
                RB_input.add(new RB_Node(Integer.valueOf(s[0]), Integer.valueOf(s[1]),nil,nil,nil, Color.black));
                i++;
            }
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.root = sortedListToRB(0, RB_input.size() -1, RB_input);
		ArrayList<ArrayList<RB_Node>> result = bfs_traversal();
		color_lowest_level(result,numOfNodes);	
	}
	
	//convert arraylist to a binary search tree with all internal nodes colored black;
	public  RB_Node sortedListToRB(int start, int end, ArrayList<RB_Node> input){
		if(start == end){
			return input.get(start);
		}
		if(start > end){
			return input.get(0).parent;
		}
		int mid = start + (end - start) / 2;
		RB_Node root = input.get(mid);
		RB_Node right = sortedListToRB(mid+1,end, input);	
		root.right = right;
		right.parent = root;
		RB_Node left = sortedListToRB(start, mid -1, input);
		root.left = left;
		left.parent = root;
		return root;
		
	}
	
	// using bfs traversal and save all nodes in an arraylist in order to find the lowest layer
	public  ArrayList< ArrayList<RB_Node>> bfs_traversal(){
		 ArrayList<ArrayList<RB_Node>> result = new ArrayList<ArrayList<RB_Node>>();
	     if (this.root == null) {
	          return result;
	     }
	     Queue<RB_Node> queue = new LinkedList<RB_Node>();
	     queue.offer(this.root);
	     while (!queue.isEmpty()) {
	    	 ArrayList<RB_Node> level = new ArrayList<RB_Node>();
	         int size = queue.size();
	         for (int i = 0; i < size; i++) {
	        	 RB_Node head = queue.poll();
	             level.add(head);
	             if (head.left != null) {
	            	 queue.offer(head.left);
	             }
	             if (head.right != null) {
	                 queue.offer(head.right);
	             }
	         }
	         result.add(level);
	    }
	     return result;
	}
	// extract the lowest layer nodes in arraylist and colored them red; this way we build a red-black tree
	public  void color_lowest_level(ArrayList<ArrayList<RB_Node>> result,int numOfNodes){
		ArrayList<RB_Node> last_level = result.get(result.size()-2);
		for(int i = 0; i < last_level.size(); i++){
			if(last_level.get(i).ID != 0){
				last_level.get(i).color = Color.red;
			}
		}	
	}

	//find the Count of certain ID, if exsits, print the count of the node, otherwise print 0;
	public  void count(int ID){
		RB_Node node = Search(ID, this.root);
		System.out.println(node.count);
				
	}
	
	//print the total count of IDs between ID1 and ID2;
	public  void inrange(int ID1, int ID2){
		  ArrayList<Integer> res = new ArrayList<Integer>();
		  helper( root, ID1, ID2,res);
		  int total = 0;
		  for(int i = 0; i < res.size(); i++){
		       total += res.get(i);
		  }
		  System.out.println(total);       
	}
	//this is a helper function to find nodes in range in recursive	way    
	private void helper(RB_Node root, int k1, int k2, ArrayList<Integer> res){
        if (root == null) {
            return;
        }
        if (root.ID > k1) {
            helper(root.left, k1, k2,res);
        }
        if (root.ID >= k1 && root.ID <= k2) {
            res.add(root.count);
        }
        if (root.ID < k2) {
            helper(root.right, k1, k2,res);
        }
    
	}
		
	//find min
	public  RB_Node getMin(RB_Node root){
		RB_Node current = root;
		while(!current.left.isNil()){
			current = current.left;	
		}
		return current;
	}
	
	//find max
	public  RB_Node getMax(RB_Node root){
		RB_Node current = root;
		while(!current.right.isNil()){
			current = current.right;
		}
		return current;
	}
	
	/*print the ID and the count of the event with the lowest ID that is greater that theID.
	Print “0 0”, if there is no next ID.*/
	public  void next(int ID){
		RB_Node current = Search(ID,root);
		if(current.isNil()){
			RB_Node tmp = this.root;
			RB_Node last = tmp;
			while(!tmp.isNil()){
				last = tmp;
				if(ID > tmp.ID){
					tmp = tmp.right;
				}
				else{ 
					tmp = tmp.left;
				}
			}
			if(last.ID > ID){
				System.out.println(last.ID + " "+last.count);
			}
			else{
				RB_Node parent = last.parent;
				while(!parent.isNil() && parent.right == last){
					last = parent;
					parent = parent.parent;
				}
				System.out.println(parent.ID+ " "+parent.count);
			}
		}
		else{
			if(!current.right.isNil()){
		        RB_Node min_node = getMin(current.right);
				System.out.println(min_node.ID+" "+min_node.count);
			}
			else{
				RB_Node parent = current.parent;
				while(!parent.isNil() && parent.right == current){
					current = parent;
					parent = parent.parent;
				}
				System.out.println(parent.ID+" "+parent.count);
			}
		}
	}
	
	//search a node, if exsits, return the node, otherwise return null;
	public  RB_Node Search(int ID, RB_Node root){
		while(!root.isNil()){
			if(root.ID == ID){
				return root;
			}
		    else if(root.ID > ID){
			    root = root.left;
		    }
			else{
				root = root.right;
			}
		}
		return root;
	}
	//given a node, print the previous node's ID and count, if its previous node doesn't exsit, print"0,0";
	public void previous(int ID){
		RB_Node current = Search(ID,root);
	//Case1: the given node doesn't exist in red black tree	
		if(current.isNil()){
			RB_Node tmp = this.root;
			RB_Node last = new Nil();
			while(!tmp.isNil()){
				last = tmp;
				if(ID > tmp.ID){
					tmp = tmp.right;
				}
				else{ 
					tmp = tmp.left;
				}
			}
	
	       //if the last node's ID is smaller than the node, it's the previous
			if(last.ID < ID){
				System.out.println(last.ID+" "+ last.count);
			}
	       //otherwise find the first ancestor where the node lies in the right subtree of it
			else{
				RB_Node parent = last;
				RB_Node child = tmp;
				while(!parent.isNil() && parent.left == child){
					child = parent;
					parent = parent.parent;
				}
				System.out.println(parent.ID + " "+ parent.count);
			}
			
		}
	//Case2:  the node exists in tree
		else{
	        //if the node has left subtree, return the maximum node in left subtree
			if(!current.left.isNil()){
				RB_Node max_node = getMax(current.left);
				System.out.println(max_node.ID + " "+max_node.count);
			}
	        //if the node doesn't have left subtree, find the first ancestor where the node lies in its right subtree
			else{
				RB_Node parent = current.parent;
				while(!parent.isNil() && parent.left == current){
					current = parent;
					parent = parent.parent;
				}
				System.out.println(parent.ID+" "+parent.count);
			}
		}
	}

   // given a node, if it doesn't exist, insert it
	public RB_Node Insert(int ID){
		RB_Node tmp = this.root;
		RB_Node last = tmp;
		while(!tmp.isNil()){
			last = tmp;
			if(ID > tmp.ID){
				tmp = tmp.right;
			}
			else{ 
				tmp = tmp.left;
			}
		}
		Nil nil = new Nil();
		RB_Node z = new RB_Node(ID,0,nil,nil,nil,Color.red);
		z.parent = last;
		if(last.isNil()){
			this.root = z;
		}
		if(last.ID > ID){
			last.left = z;
		}
		else{
			last.right = z;
		}
		return z;
	}
    // given a node, if it's present, increase its count by m, otherwise, call the inser function to insert the node;
	public void increase(int ID, int m){
		RB_Node node = Search(ID,this.root);
		if(!node.isNil()){
			node.count += m;
			System.out.println(node.count);
		}
	    else{
			node = Insert(ID);
			node.count = m;
			System.out.println(node.count);
		}		
		RB_insert_fixup(node);
	}
	// left rotate of a given node;	
	public void leftRotate(RB_Node x){
		RB_Node y = x.right;
		x.right = y.left;
		if(!y.left.isNil()){
			y.left.parent = x;
		}
		if(x.parent.isNil()){
			this.root = y;
			y.parent = x.parent;
		}
		else if(x == x.parent.left){
			x.parent.left = y;
			y.parent = x.parent;
		}
		else{
			x.parent.right = y;
			y.parent = x.parent;
		}
		y.left = x;
		x.parent = y;
			
	}
	//right rotate of a given node;
	public void rightRotate(RB_Node y){
		RB_Node x = y.left;
		y.left = x.right;
		if(!x.right.isNil()){
			x.right.parent = y;	
		}
		if(y.parent.isNil()){
			this.root = x;
			x.parent = y.parent;
		}
		else if(y == y.parent.left){
			y.parent.left = x;
			x.parent = y.parent;
		}
		else{
			y.parent.right = x;
			x.parent = y.parent;
		}
		x.right = y;
		y.parent = x;
	}
   // fix up the red black tree to satisfy the properties after insertion
	private void RB_insert_fixup(RB_Node node) {
		while(node.parent.color == Color.red){
	    //node's parent is the left child
			if(node.parent == node.parent.parent.left){
				RB_Node uncle = node.parent.parent.right;
				if(uncle.color == Color.red ){
					node.parent.color = Color.black;	//case1: node's uncle is red
					uncle.color = Color.black;
					node.parent.parent.color = Color.red;
					node = node.parent.parent;
				}
				else if(node == node.parent.right){		//case2: node's uncle is black and is right chile
					node = node.parent;
					leftRotate(node);
				}
				node.parent.color = Color.black;		//case3: node's uncle is black and is left child
				node.parent.parent.color = Color.red;
				rightRotate(node.parent.parent);
						
			}
			//node's parent is right child
			else{
				RB_Node uncle = node.parent.parent.left;
				if(uncle.color == Color.red ){
					node.parent.color = Color.black;
					uncle.color = Color.black;
					node.parent.parent.color = Color.red;
					node = node.parent.parent;
				}
				else if(node == node.parent.left){
					node = node.parent;
					rightRotate(node);
				}
				node.parent.color = Color.black;
				node.parent.parent.color = Color.red;
				leftRotate(node.parent.parent);	
			}	
		}
				
		this.root.color = Color.black;
	}
	//replace the given node u with node v;
	public void transplant_RB(RB_Node u, RB_Node v){
		if(u.parent.isNil()){
			this.root = v;
		}
		else if(u == u.parent.left){
			u.parent.left = v;
		}
		else{
			u.parent.right = v;
		}
		v.parent = u.parent;
	}
	/* if the given node's count is less or equal to 0 after decreased by m, delete it.
	This function is for node's deletion*/
	public void Delete_RB(RB_Node z){
		RB_Node y = z;
		RB_Node copy_y = y;
		RB_Node x;
		copy_y.color = y.color;
		if(z.left.isNil()){
			x = z.right;
			transplant_RB(z,z.right);
		}
		else if(z.right.isNil()){
			x= z.left;
			transplant_RB(z,z.left);
		}
		else{
			y = getMin(z.right);
		    x = y.right;
		    copy_y.color = y.color;
			transplant_RB(y,y.right);
			transplant_RB(z,y);
			y.left = z.left;
			y.left.parent = y;
			y.right = z.right;
			y.right.parent = y;
			y.color = z.color;
		}	
		if(copy_y.color == Color.black){
			Delete_RB_fixup(x);
		}
	}
	//After deletion, do the fixup to satisfy the property of red black tree
	public void Delete_RB_fixup(RB_Node x){
		while(x != this.root && x.color == Color.black){
			//x is left child
			if(x == x.parent.left){
				RB_Node w = x.parent.right;
				if(w.color == Color.red){			//Case1: x’s sibling w is red
					w.color = Color.black;
					x.parent.color = Color.red;
					leftRotate(x.parent);
					w = x.parent.right;
				}
				if(w.left.color == Color.black  &&  w.right.color == Color.black){
					w.color = Color.red;			//Case 2: x’s sibling w is black, and both of w’s children are black
					x = x.parent;
				}
				else{
					if(w.right.color == Color.black){//Case 3: x’s sibling w is black, 
													//w’s left child is red, and w’s right child is black
						w.left.color = Color.black;
						w.color = Color.red;
						rightRotate(w);
						w = x.parent.right;
					}
					w.color = x.parent.color;		//Case 4: x’s sibling w is black, and w’s right child is red
					x.parent.color = Color.black;
					w.right.color = Color.black;
					leftRotate(x.parent);
					x = this.root;
						
				}
			}
			//x is right child
			else if(x == x.parent.right){
				RB_Node w = x.parent.left;
				if(w.color == Color.red){
					w.color = Color.black;
					x.parent.color = Color.red;
					rightRotate(x.parent);
					w = x.parent.left;
				}
				if(w.left.color == Color.black  &&  w.right.color == Color.black){
					w.color = Color.red;
					x = x.parent;
				}
				else{ 
					if(w.left.color == Color.black){
						w.right.color = Color.black;
						w.color = Color.red;
						leftRotate(w);
						w = x.parent.left;
					}
					w.color = x.parent.color;
					x.parent.color = Color.black;
					w.left.color = Color.black;
					rightRotate(x.parent);
					x = this.root;
				}
						
			}
					
		}
			x.color = Color.black;
	}
	/*if the given node is present, decrease the node's count by m, if the count is less than 0 after deletion,
	call the delete function*/
	public void reduce(int ID, int m){
		RB_Node node = Search(ID, this.root);
		if(node.isNil()){
			System.out.println("0");
		}
		else{
			if(node.count - m <= 0){
				System.out.println("0");
				Delete_RB(node);	
			}
			else{
				node.count -= m;
				System.out.println( node.count );
			}
		}
	}
}

class bbst {
	public static void main(String[] args){
		RB_tree RB = new RB_tree();
		RB.createTree(args[0]);
		Scanner in = new Scanner(System.in);
		String command ="";
		while(in.hasNext()){
			command = in.nextLine();
			String[] s = command.split(" ");
			if(s[0].equals("quit")){
				break;	
			}
			if(s[0].equals("increase")){
				RB.increase(Integer.parseInt(s[1]), Integer.parseInt(s[2]));
			}
			if(s[0].equals("reduce")){
				RB.reduce(Integer.parseInt(s[1]), Integer.parseInt(s[2]));
			}
			if(s[0].equals("count")){
				RB.count(Integer.parseInt(s[1]));
			}
			if(s[0].equals("inrange")){
				RB.inrange(Integer.parseInt(s[1]), Integer.parseInt(s[2]));
			}
			if(s[0].equals("next")){
				RB.next(Integer.parseInt(s[1]));
			}
			if(s[0].equals("previous")){
				RB.previous(Integer.parseInt(s[1]));
			}
		}
	}
}
	
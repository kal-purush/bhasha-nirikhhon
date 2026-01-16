package LinkedList;

public class LinkedListNode {
	int node;
	LinkedListNode next;
	public LinkedListNode(int node, LinkedListNode next) {
		// TODO Auto-generated constructor stub
		this.node = node;
		this.next  = next;
	}
	public int getNode() {
		return node;
	}
	
	public LinkedListNode getNext() {
		return next;
	}
}
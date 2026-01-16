package stack;

public class NodeStack<TYPE> {
	
	private TYPE data;
	private NodeStack<TYPE> next;
	
	public NodeStack(TYPE data) {
		this.data = data;
	}

	public NodeStack(TYPE data, NodeStack<TYPE> next) {
		this.data = data;
		this.next = next;
	}
	
	public TYPE getData() {
		return data;
	}
	public void setData(TYPE data) {
		this.data = data;
	}
	public NodeStack<TYPE> getNext() {
		return next;
	}
	public void setNext(NodeStack<TYPE> next) {
		this.next = next;
	}
	
}
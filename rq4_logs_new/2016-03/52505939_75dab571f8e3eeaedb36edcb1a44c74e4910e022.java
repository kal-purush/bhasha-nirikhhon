package examples;

/**
 * @author ps
 * Cylic array buffer implementation of a Queue
 * @param <E> the type of the objects to be stored in this Queue
 */
public class MyQueue<E> implements Queue<E> { 
	
	private E[] stor = (E[]) new Object[256]; 
	private int inPtr, outPtr, size; 
	
	@Override
	public void enqueue(E o) {
		// TODO Auto-generated method stub

	}

	@Override
	public E dequeue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E head() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
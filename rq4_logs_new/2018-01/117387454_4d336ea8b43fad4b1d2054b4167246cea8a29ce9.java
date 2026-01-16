package s.Calculator;


import java.util.ArrayList;
import java.util.NoSuchElementException;

public class Stack<T> {
	private ArrayList<T> items;

	public Stack() {
		items = new ArrayList<T>();
	}

	//pushes to stack
	public void push(T item) {
		items.add(item);
	}

	//pop item at top of stack and returns it. Throws NoSuchElementException if stack is empty
	public T pop() 
	throws NoSuchElementException{
		if (items.isEmpty()) {
			//return null;
			throw new NoSuchElementException("can't pop from an empty stack");
		}
		return items.remove(items.size()-1);
	}

	//returns item at top without popping it. Throws NoSuchElementException If stack is empty.
	public T peek() 
	throws NoSuchElementException {
		if (items.size() == 0) 
		{
			//return null;
			throw new NoSuchElementException("can't peek on an empty stack");
		}
		return items.get(items.size()-1);
	}

	//tells if stack is empty
	public boolean isEmpty() {
		return items.isEmpty();
	}

	//returns number of items in stack
	public int size() {
		return items.size();
	}

	//empties stack
	public void clear() {
		items.clear();
	}
}
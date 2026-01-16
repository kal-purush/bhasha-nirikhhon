package parsing;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LRUCache<T> {

	private LinkedList<T> buffer;
	private int capacity;

	public LRUCache(int capacity) {
		this.buffer = new LinkedList<>();
		this.capacity = capacity;
	}

	public boolean isFull() {
		return buffer.size() == capacity;
	}

	public T add(T element) {
		T result = null;
		if (isFull()) {
			result = buffer.poll();
		}
		buffer.add(element);
		return result;
	}

	public List<T> getFullBuffer() {
		return this.buffer;
	}

	public static void main(String[] args) {
		LRUCache<Integer> buffer = new LRUCache<Integer>(10);

		for (int i = 0; i < 10; i++) {
			buffer.add(i);
		}

		System.out.println(toString(buffer.getFullBuffer()));

		buffer.add(111);

		System.out.println(toString(buffer.getFullBuffer()));
	}

	private static String toString(List<Integer> fullBuffer) {
		StringBuffer result = new StringBuffer();
		Iterator<Integer> it = fullBuffer.iterator();
		if (it.hasNext()) {
			result.append(it.next());

			while (it.hasNext()) {
				result.append(", " + it.next());
			}
		}
		return result.toString();
	}
}
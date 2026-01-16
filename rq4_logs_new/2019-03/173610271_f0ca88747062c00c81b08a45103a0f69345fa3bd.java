import java.util.LinkedList;

public class ChainingHT {

	public static void main(String[] args) {
		ChainingHT map = new ChainingHT(4);
		map.put(1, "Hi");
		map.put(5, "bye");
		map.print();
		
		System.out.println("---------------------------");
		
	}

	class HashElement {
		private Object value;
		private Object key;

		public HashElement(Object key, Object value) {
			this.key = key;
			this.value = value;
		}
	}

	private LinkedList<HashElement>[] hashArray;
	private int capacity;
	private int size;

	public ChainingHT(int capacity) {
		this.capacity = capacity;
		this.hashArray = new LinkedList[this.capacity];
		this.size = 0;
		for(int i = 0; i < this.capacity; i++) {
			hashArray[i] = new LinkedList<HashElement>();
		}
	}
	
	//Implement for HashSet
	public void put(Object key) {
		put(key, null);
	}
	public void put(Object key, Object value) {
		int index = this.hash(key);
		HashElement temp = new HashElement(key, value);
		hashArray[index].add(temp);
		size++;
	}
	
	public Object get(Object key) {
		int index = hash(key);
		LinkedList<HashElement> temp = hashArray[index];
		for(HashElement e: temp) {
			if(e.key.equals(key))
				return e.value;
		}
		return null;
	}
	
	public boolean contains(Object key) {
		int index = hash(key);
		LinkedList<HashElement> temp = hashArray[index];
		for(HashElement e: temp) {
			if(e.key.equals(key))
				return true;
		}
		return false;
	}
	
	public void print() {
		int index = 0;
		for(LinkedList<HashElement> l: hashArray) {
			System.out.format("[%d]", index++);
			l.forEach(e -> System.out.format("(%s, %s)", e.key,e.value));
			System.out.println();
		}
	}
	
	public int hash(Object key) {
		return Math.abs(key.hashCode()) % this.capacity;
	}
	
}
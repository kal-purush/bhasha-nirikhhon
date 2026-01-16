import java.util.Iterator;

//This suppresses warnings we may get
@SuppressWarnings("unchecked")
public class ArraySet<T> implements Set<T> {


	public static final int CAPACITY_MULTIPLIER = 2;
	public static final int DEFAULT_CAPACITY = 15;
	
	private int size = 0;
	private int numberInSet = 0;
	private T[] sets;
	
	
    public ArraySet() {
    	this(DEFAULT_CAPACITY);
    }

    public ArraySet(int size) {
		if (size < 0) {
			throw new IllegalArgumentException("Size of the set must be >= 0");
		}
		
		this.size = size;
		sets = (T[])new Object[size];
    }
    
	
	@Override
	public void add(T element) {
		if(!(this.contains(element))) {
			ensureCapacity();
			sets[numberInSet] = element;
			numberInSet++;
		}
		else {
			System.out.println("This element already exists in the set");
		}
	}

	@Override
	public void addAll(T[] elements) {
		for (int i = 0; i < elements.length; i++) {
			add(elements[i]);	
		}
	}

	@Override
	public boolean contains(T element) {
		if (indexOf(element) > -1)
			return true;
		else
			return false;
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return numberInSet;
	}

	@Override
	public void remove(T element) {
		int index = indexOf(element);
		
		if (index > -1) {
			numberInSet--;
			sets[index] = sets[numberInSet];
		}

	}

	@Override
	public Set<T> union(Set<T> anotherSet) {			

		Set<T> unionSet = new ArraySet<T>();
		
		unionSet = anotherSet.difference(this);

		for(int i = 0; i < numberInSet; i++) {
			unionSet.add(sets[i]);
		}
		
		return unionSet;
				
		}


	@Override
	public Set<T> intersection(Set<T> anotherSet) {
		Set<T> intersectSet = new ArraySet<T>();
		for(int i = 0; i < numberInSet; i++) {
			if(anotherSet.contains(sets[i])) {
				intersectSet.add(sets[i]);
			}
		}
		
		return intersectSet;
	}

	@Override
	public Set<T> difference(Set<T> anotherSet) {

		Set<T> diffSet = new ArraySet<T>();
		for(int i = 0; i < numberInSet; i++) {
			if(!(anotherSet.contains(sets[i]))) {
				diffSet.add(sets[i]);
			}
		}
		
		return diffSet;
	}
	
	private int indexOf(T element) {
		int index = -1;
		for (int i = 0; i < numberInSet; i++) {
			if (sets[i].equals(element)) {
				index = i;
				break;
			}
		}
		
		return index;
	}
	
	private void ensureCapacity() {
		if (numberInSet == size) {
			T[] newArray = (T[]) new Object[(numberInSet+1) * CAPACITY_MULTIPLIER];
			System.arraycopy(sets,0,newArray,0,numberInSet);
			sets = newArray;
		}
	}
	
    public Iterator<T> iterator(){
    	
    	return new ArraySetIterator();
	}
	
	/**
	 * Inner class that generates an iteration of the bag.
	 */
	private class ArraySetIterator implements Iterator<T>
	{
		private int index = 0;
		
		/**
		 * Determines if there are more elements
		 * in the iteration.
		 * 
		 * @return true if there are more elements, false otherwise.
		 */
		public boolean hasNext() {
			if (index < numberInSet)
				return true;
			else
				return false;
		}

		/**
		 * Returns the next element in the iteration.
		 * 
		 * @throws java.util.NoSuchElementException if there are no more elements in the iteration.
		 */
		public T next() {
			if (hasNext()) {
				T nextItem = sets[index];
				index++;
				
				return nextItem;
			}
			else
				throw new java.util.NoSuchElementException("No items remaining in the iteration.");
			
		}

		/**
		 * The remove() operation is not supported.
		 * @throws UnsupportedOperationException if involed.
		 */
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}



}

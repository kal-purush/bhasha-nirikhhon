package root;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * <code>MyArrayList</code> is a data-structure I created to mimic the java.util.ArrayList<T> class. It behaves in 
 * the same manner, and allows for generic types.
 * @author Joseph Abad
 * @param <T>
 */
public class MyArrayList<T> implements Collection<T>, List<T>{
	private int capacity;
	private int size;
	private T[] array;
	
	///////////////////////////////////////////////////////////
	//Constructors
	///////////////////////////////////////////////////////////
	public MyArrayList() {
		this.capacity = 11;
		this.size = 0;
		array = (T[]) new Object[this.capacity];
	}
	public MyArrayList(int capacity) {
		this.capacity = capacity;
		this.size = 0;
		array = (T[]) new Object[this.capacity];
	}
	
	
	
	///////////////////////////////////////////////////////////
	//Standard Methods
	///////////////////////////////////////////////////////////
	@Override
	public boolean add(T e) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void add(int index, T element) {
		// TODO Auto-generated method stub
	}
	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public T get(int index) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public T remove(int index) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public boolean addAll(Collection<? extends T> c) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	
	
	
	///////////////////////////////////////////////////////////
	//Special Methods
	///////////////////////////////////////////////////////////
	public void ensureCapacity(int minCapacity) {
		
	}
	@Override
	public Iterator<T> iterator() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ListIterator<T> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ListIterator<T> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}	
	@Override
	public T set(int index, T element) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	///////////////////////////////////////////////////////////
	//Common Methods
	///////////////////////////////////////////////////////////
	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void clear() {
		// TODO Auto-generated method stub
	}
	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	
	///////////////////////////////////////////////////////////
	//Overrides
	///////////////////////////////////////////////////////////
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Arrays.toString(array);
	}
	/**
	 * @see java.lang.Object#equals()
	 */
	@Override
	public boolean equals(Object o) {
		return false;
	}
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	//Testing
	private T[] getArray() {return array;}
}
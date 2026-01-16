import java.util.ArrayList;
import java.util.List;

public class ArrayListHeapPQ<K extends Comparable<K>, E> extends MyPQ<K, E> 
{
	List<MyPair<K, E>> elements = new ArrayList<MyPair<K, E>>();
	
	@Override
	boolean insert(E ElementType, K KeyType) 
	{
		elements.add(new MyPair(KeyType, ElementType));
		sortUp(elements.size() - 1);
		return true;
	}

	@Override
	E min()
	{
		return elements.get(0).e;
	}

	@Override
	E remove()
	{
		swap(0, elements.size() - 1);
		MyPair<K, E> tmp = elements.remove(elements.size() - 1);
		
		sortDown(0);
		
		return tmp.e;
	}

	@Override
	List<MyPair<K, E>> getElements()
	{
		return elements;
	}
	
	@Override
	int getSize()
	{
		return elements.size();
	}
	
	void swap(int i, int j)
	{
		MyPair<K,E> tmp = elements.get(i);
		
		elements.set(i, elements.get(j));
		elements.set(j, tmp);
	}
	
	void sortDown(int i)
	{
		int left = leftChild(i);
		int right = rightChild(i);
		
		//holds the position of the smallest element
		int min = i;

		if (left <= elements.size()-1 && elements.get(left).k.compareTo(elements.get(i).k) < 0)
			min = left;
		if (right <= elements.size()-1 && elements.get(right).k.compareTo(elements.get(min).k) < 0)
			min = right;
		
		if (min != i)
		{
			swap(min, i);
			sortDown(min);
		}
	}
	
	void sortUp(int i)
	{
		if (i > 0 && elements.get(i).k.compareTo(elements.get(parent(i)).k) < 0) {
			swap(i, parent(i));
			sortUp(parent(i));
		}
	}
			
	private int leftChild(int i) {
		return 2*i + 1;
	}

	private int rightChild(int i) {
		return 2*i + 2;
	}

	private int parent(int i) {
		return (i-1)/2;
	}
}
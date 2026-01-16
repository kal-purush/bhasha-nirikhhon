package examples;

import java.util.Iterator;

public class MyLinkedList<E> implements List<E> {
	// auxiliary class for our positions
	class LNode implements Position<E>{
		E elem;
		LNode prev,next;
		Object owner = MyLinkedList.this; //reference to the creator-object
		
		@Override
		public E element() {
			return elem;
		}
		
	}
	 
	private LNode first,last;
	private int size; 
	
	private MyLinkedList<E>.LNode checkAndCast(Position<E> p) {
		LNode n;
		try {
			n = (LNode) p;
		} catch (ClassCastException e) {
			throw new RuntimeException("This is not a Position belonging to MyLinkedList"); 
		}
		if (n.owner == null) throw new RuntimeException("position was allready deleted!");
		if (n.owner != this) throw new RuntimeException("position belongs to another MyLinkedList instance!");			
		return n;
	}


	
	@Override
	public Position<E> first() {
		return first;
	}

	@Override
	public Position<E> last() {
		return last;
	}

	@Override
	public boolean isFirst(Position<E> p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast(Position<E> p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Position<E> next(Position<E> p) {
		LNode n = checkAndCast(p);
		return n.next;
	}


	@Override
	public Position<E> previous(Position<E> p) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E replaceElement(Position<E> p, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> insertFirst(E o) {
		LNode n = new LNode();
		n.elem = o;
		if (first == null){
			last = n;
		}
		else {
			first.prev = n;
			n.next = first;
		}
		first = n;
		size++;
		return n;
	}

	@Override
	public Position<E> insertLast(E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> insertBefore(Position<E> p, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> insertAfter(Position<E> p, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(Position<E> p) {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<Position<E>> positions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<E> elements() {
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
		List<String> li = new MyLinkedList<>(); 
		Position<String> p1 = li.insertFirst("hans");
		li.insertFirst("susi");
		li.insertFirst("beat");
		p1 = li.insertFirst("urs");
		while (p1 != null) {
			System.out.println(p1.element());
			p1 = li.next(p1);
		}
//		System.out.println(p1.element());
//		System.out.println(p2.element());
//		Iterator<String> it = li.elements();
//		while (it.hasNext()) System.out.println(it.next());
	}

}
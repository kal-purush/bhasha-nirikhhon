package examples;

import java.util.Iterator;

public class MyTree<E> implements Tree<E> {

	class TNode implements Position<E>{

		E elem;
		TNode parent;
		MyLinkedList<TNode> children = new MyLinkedList<>();
		Position<Position> myChildrenPosition; 
		Object owner = MyTree.this;
		
		@Override
		public E element() {
			return elem;
		}
		
	}
	
	private TNode root;
	private int size;
	
	@Override
	public Position<E> root() {
		return root;
	}

	@Override
	public Position<E> createRoot(E o) {
		root = new TNode();
		root.elem = o;
		size++;
		return root;
	}

	@Override
	public Position<E> parent(Position<E> child) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Position<E>> childrenPositions(Position<E> parent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<E> childrenElements(Position<E> parent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int numberOfChildren(Position<E> parent) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Position<E> insertParent(Position<E> p, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> addChild(Position<E> parent, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> addChildAt(int pos, Position<E> parent, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> addSiblingAfter(Position<E> sibling, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Position<E> addSiblingBefore(Position<E> sibling, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(Position<E> p) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isExternal(Position<E> p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isInternal(Position<E> p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public E replaceElement(Position<E> p, E o) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
package genex.utils;

import java.io.Serializable;
import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 
 * Lists are time and space consuming and has lot of cache miss ratio If Lists
 * are very big then it is difficult to utilize RAM and parallism <br>
 * Arrays are good solution but require continuous memory <br>
 * If Arrays are very large then continous mem is a problem dynamic size growing
 * is also a problem <br>
 * 
 * So, combining arrays with lists is good solution <br>
 * It will be a List of Arrays, User specify size of array in each node <br>
 * Each Node of list will contain a single Array, so memory wasting in pointer
 * managing will reduce significantly <br>
 * 
 * Class Itself will manage all the arrays in all the nodes, So on higher level
 * All arrays will be accessed as a single array but only storing way will
 * change.. <br>
 * +-----------------------+-+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+-------------
 * ---------<br>
 * |&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|
 * <br>
 * | Array ref of size [n]&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|
 * -+----+---> nxt node<br>
 * |&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * &nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|
 * <br>
 * +------------------------+-+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+------------------
 * ----<br>
 */

public class LinkedArrayList<E> extends AbstractSequentialList<E>implements List<E>, Deque<E>, Cloneable, Serializable {
	transient private int size = 0;
	transient private Node<E> first = null;
	transient private Node<E> last = null;
	private transient int bufferSize = 25;
	private transient int fSize = 0;
	private transient int lSize = 0;
	private static final long serialVersionUID = 123456789L;

	public LinkedArrayList() {
	}

	public LinkedArrayList(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public LinkedArrayList(Collection<? extends E> paramCollection) {
		this();
		addAll(paramCollection); // FIXME
	}

	public LinkedArrayList(int bufferSize, Collection<? extends E> paramCollection) {
		this(bufferSize);
		addAll(paramCollection); // FIXME
	}

	public int size() {
		return size;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	private void linkFirst(E paramE) {
		if (first == null) {
			Node<E> node = new Node<E>(null, bufferSize, null);
			first = last = node;
			fSize = 0; // Reset if wrongly set
			node.data[fSize++] = paramE;
			++size;
		} else if (first.data == null) { // It will never exists, only when, if
											// somehow user set data with null
			first.data = new Object[bufferSize];
			fSize = 0; // reset size
			first.data[fSize++] = paramE;
			++size;
		} else if (first.data.length == fSize) {// first node is full, then we
												// need a new one
			Node<E> node = new Node<E>(null, bufferSize, first);
			fSize = 0; // reset size for new node
			node.data[fSize++] = paramE;
			++size;
		} else {
			first.data[fSize++] = paramE;
			++size;
		}
	}

	private void linkLast(E paramE) {
		if (last == null) {
			Node<E> node = new Node<E>(null, bufferSize, null);
			first = last = node;
			lSize = 0; // Reset if wrongly set
			node.data[lSize++] = paramE;
			++size;
		} else if (last.data == null) { // It will never exists, only when, if
										// somehow user set data with null
			last.data = new Object[bufferSize];
			lSize = 0; // reset size
			last.data[lSize++] = paramE;
			++size;
		} else if (last.data.length == lSize) {// Last node is full, then we
												// need a new one
			Node<E> node = new Node<E>(last, bufferSize, null);
			lSize = 0; // reset size for new node
			node.data[lSize++] = paramE;
			++size;
		} else {
			last.data[lSize++] = paramE;
			++size;
		}
	}

	void linkBefore(E paramE, Node<E> paramNode) { // TODO FIXME
		throw new UnsupportedOperationException("Linking in between is not supported in " + this.getClass().getName());
	}

	private E unlinkFirst(Node<E> paramNode) {
		if (size == 0 || first == null) {
			throw new NoSuchElementException();
		} else if (first.data == null || fSize == 0) {
			if (first.next == null) { // that was the last node with no element,
										// raise exception after deleting it
				first.data = null;
				first = null;
				throw new NoSuchElementException();
			} else { //this condition should never happens
				first.next.prev = null; // delete current reference to first
										// node
				Node<E> localNode = first;
				first = first.next; // move to next Node
				localNode.data = null; // free the storage
				localNode.next = null; // delete reference to current list also
				localNode = null; // delete node
				unlinkFirst(first); // doit recursive until we don't find any
									// element
			}
		} else {
//TODO
			
		}
		return null;
	}

	@SuppressWarnings({ "unchecked", "unused", "hiding" })
	private class Node<E> {
		Object[] data;
		Node<E> next;
		Node<E> prev;

		private Node() {
		}

		Node(Node<E> prev, int length, Node<E> next) {
			this.next = next;
			this.prev = prev;
			if (next != null)
				next.prev = this;
			if (prev != null)
				prev.next = this;
			this.data = new Object[length];

		}

		Node(Node<E> prev, E[] data, Node<E> next) {
			this.data = data;
			this.next = next;
			this.prev = prev;
			if (next != null)
				next.prev = this;
			if (prev != null)
				prev.next = this;

		}

	}

}
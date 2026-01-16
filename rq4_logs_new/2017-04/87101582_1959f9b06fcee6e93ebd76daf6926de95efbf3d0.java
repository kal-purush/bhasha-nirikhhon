package com.cybergeass.utils.LinkedArrayList;

import java.io.Serializable;
import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

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
			} else { // this condition should never happens
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
			// TODO

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

	@Override
	public void addFirst(E paramE) {
		// TODO FIXME
		
	}

	@Override
	public void addLast(E paramE) {
		// TODO FIXME
		
	}

	@Override
	public Iterator<E> descendingIterator() {
		// TODO FIXME
		return null;
	}

	@Override
	public E element() {
		// TODO FIXME
		return null;
	}

	@Override
	public E getFirst() {
		// TODO FIXME
		return null;
	}

	@Override
	public E getLast() {
		// TODO FIXME
		return null;
	}

	@Override
	public boolean offer(E paramE) {
		// TODO FIXME
		return false;
	}

	@Override
	public boolean offerFirst(E paramE) {
		// TODO FIXME
		return false;
	}

	@Override
	public boolean offerLast(E paramE) {
		// TODO FIXME
		return false;
	}

	@Override
	public E peek() {
		// TODO FIXME
		return null;
	}

	@Override
	public E peekFirst() {
		// TODO FIXME
		return null;
	}

	@Override
	public E peekLast() {
		// TODO FIXME
		return null;
	}

	@Override
	public E poll() {
		// TODO FIXME
		return null;
	}

	@Override
	public E pollFirst() {
		// TODO FIXME
		return null;
	}

	@Override
	public E pollLast() {
		// TODO FIXME
		return null;
	}

	@Override
	public E pop() {
		// TODO FIXME
		return null;
	}

	@Override
	public void push(E paramE) {
		// TODO FIXME
		
	}

	@Override
	public E remove() {
		// TODO FIXME
		return null;
	}

	@Override
	public E removeFirst() {
		// TODO FIXME
		return null;
	}

	@Override
	public boolean removeFirstOccurrence(Object paramObject) {
		// TODO FIXME
		return false;
	}

	@Override
	public E removeLast() {
		// TODO FIXME
		return null;
	}

	@Override
	public boolean removeLastOccurrence(Object paramObject) {
		// TODO FIXME
		return false;
	}

	@Override
	public ListIterator<E> listIterator(int paramInt) {
		// TODO FIXME
		return null;
	}

}
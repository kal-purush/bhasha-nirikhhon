package com.oriaxx77.algorythm.list;

import java.util.Objects;

/**
 * Reverse a list of {@link Node} objects in place.
 */
public class LinkedListReversal {
	
	/**
	 * Reverse the list of nodes and returns with the new head.
	 * @param head The list of {@link Node} objects to reverse
	 * @return The head of the reversed list.
	 */
	public <T> Node<T> reverse( Node<T> head ){
		Objects.requireNonNull( head );
		
		Node<T> prev = null;
		Node<T> current = head;
		Node<T> next = null;
		while( current != null ){
			next = current.getNext();
			current.setNext( prev );
			prev = current;
			current = next;
			
		}
		return prev;
	}
	
}
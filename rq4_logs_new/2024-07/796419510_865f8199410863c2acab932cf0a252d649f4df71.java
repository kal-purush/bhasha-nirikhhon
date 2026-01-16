package org.jal.collections.list;

import java.util.NoSuchElementException;

public class FifoLinkedList<T> {
  private LinkedNode<T> head;
  private LinkedNode<T> tail;
  private int size;

  public FifoLinkedList() {
    this.head = new LinkedNode<>(); // sentinel
    this.tail = this.head;
    this.size = 0;
  }

  public T getFirst() {
    this.throwIfEmpty();

    return this.head.getNext().getData();
  }

  public void append(T data) {
    LinkedNode<T> node = new LinkedNode<>();
    node.setData(data);

    this.tail.setNext(node);
    this.tail = node;

    this.size++;
  }

  public void remove() {
    this.throwIfEmpty();

    this.head = this.head.getNext();

    this.size--;
  }

  public int getSize() {
    return this.size;
  }

  public boolean isEmpty() {
    return this.size == 0;
  }

  private void throwIfEmpty() {
    if (this.size == 0) {
      throw new NoSuchElementException();
    }
  }
}
package org.jal.collections.list;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.function.Predicate;

public class CircularLinkedList<T> implements Iterable<T> {
  private LinkedNode<T> end;
  private int size;
  private int modCount;

  public CircularLinkedList() {
    // sentinel
    this.end = new LinkedNode<>();
    // the next of the end is always the head
    this.end.setNext(this.end);

    this.size = 0;
    this.modCount = 0;
  }

  public T get(Predicate<T> pred) {
    LinkedNode<T> prev = this.findPrev(pred);
    if (prev == null) {
      return null;
    }

    LinkedNode<T> node = prev.getNext();
    return node.getData();
  }

  public boolean has(Predicate<T> pred) {
    return this.findPrev(pred) != null;
  }

  public void prepend(T data) {
    LinkedNode<T> head = this.end.getNext();
    LinkedNode<T> node = new LinkedNode<>(data, head);

    // prepend node as a new head
    node.setNext(head);
    this.end.setNext(node);

    this.size++;
    this.modCount++;
  }

  public void change(Predicate<T> pred, T data) {
    LinkedNode<T> prev = this.findPrev(pred);
    if (prev == null) {
      return;
    }

    LinkedNode<T> toChange = prev.getNext();
    toChange.setData(data);

    this.modCount++;
  }

  public void remove(Predicate<T> pred) {
    LinkedNode<T> prev = this.findPrev(pred);
    if (prev == null) {
      return;
    }

    LinkedNode<T> toRemove = prev.getNext();
    prev.setNext(toRemove.getNext());

    this.size--;
    this.modCount++;
  }

  private LinkedNode<T> findPrev(Predicate<T> pred) {
    LinkedNode<T> prev = this.end; // end is just before the head

    while (true) {
      LinkedNode<T> node = prev.getNext();
      if (node == this.end) {
        return null;
      }

      if (pred.test(node.getData())) {
        return prev;
      }

      prev = node;
    }
  }

  public int getSize() {
    return this.size;
  }

  public boolean isEmpty() {
    return this.size == 0;
  }

  private class ListIterator<U> implements Iterator<U> {
    private LinkedNode<U> node;
    private int expectedModCount;

    public ListIterator(LinkedNode<U> head) {
      this.node = head;
      this.expectedModCount = modCount;
    }

    public boolean hasNext() {
      return this.node != end;
    }

    public U next() {
      this.throwIfModified();

      U data = this.node.getData();

      this.node = this.node.getNext();

      return data;
    }

    private void throwIfModified() {
      if (modCount != this.expectedModCount) {
        throw new ConcurrentModificationException();
      }
    }
  }

  public Iterator<T> iterator() {
    LinkedNode<T> head = this.end.getNext();
    return new ListIterator<>(head);
  }
}
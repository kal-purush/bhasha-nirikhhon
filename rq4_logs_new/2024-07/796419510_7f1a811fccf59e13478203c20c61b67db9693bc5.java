package org.jal.collections.queue;

import java.util.Comparator;

import org.jal.collections.array.DynamicArray;
import org.jal.collections.heap.Heapifier;

public class HeapPriorityQueue<T> implements Queue<T> {
  private DynamicArray<T> arr;
  private Comparator<T> comp;
  private Heapifier<T> heapifier;

  public HeapPriorityQueue(Comparator<T> comp) {
    this.arr = new DynamicArray<>();
    this.comp = comp;
    this.heapifier = new Heapifier<>(this.arr::get, this.arr::set, this.comp);
  }

  public void add(T value) {
    this.arr.append(value);
    this.heapifier.siftUp(this.arr.getSize()-1, 0);
  }

  public T poll() {
    T toPoll = this.arr.get(0);
    this.swap(0, this.arr.getSize()-1);

    this.arr.remove();
    this.heapifier.siftDown(0, 0, this.arr.getSize()-1);

    return toPoll;
  }

  public T peek() {
    return arr.get(0);
  }

  public int getSize() {
    return arr.getSize();
  }

  public boolean isEmpty() {
    return arr.getSize() == 0;
  }

  private void swap(int i, int j) {
    T temp = this.arr.get(i);
    this.arr.set(i, this.arr.get(j));
    this.arr.set(j, temp);
  }
}
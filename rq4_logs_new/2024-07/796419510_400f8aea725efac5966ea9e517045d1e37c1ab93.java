package org.jal.collections.dictionary;

import java.util.function.Function;
import java.util.function.Predicate;

import org.jal.collections.list.CircularLinkedList;

public class AssocList<K, V> implements Dictionary<K, V> {
  private CircularLinkedList<V> list;
  private Function<V, K> getKey;

  public AssocList(Function<V, K> getKey) {
    this.list = new CircularLinkedList<>();
    this.getKey = getKey;
  }

  public V get(K key) {
    return this.list.get(this.hasEqualKey(key));
  }

  public void set(V value) {
    K key = this.getKey.apply(value);

    if (this.list.has(this.hasEqualKey(key))) {
      this.list.change(this.hasEqualKey(key), value);
    } else {
      this.list.prepend(value);
    }
  }

  public void remove(K key) {
    this.list.remove(hasEqualKey(key));
  }

  public int getSize() {
    return this.list.getSize();
  }

  public Iterable<V> getValues() {
    return this.list;
  }

  private Predicate<V> hasEqualKey(K key) {
    return element -> this.getKey.apply(element).equals(key);
  }
}
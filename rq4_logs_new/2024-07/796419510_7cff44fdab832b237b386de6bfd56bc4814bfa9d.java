package org.jal.collections.dictionary;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jal.collections.list.CircularLinkedList;

abstract class AbstractChainingTable<K, V> implements Dictionary<K, V> {
  protected static int INIT_NUM_SLOTS = 16;

  protected AssocList<K, V>[] slots;
  protected Function<V, K> getKey;
  protected int numValues;

  public AbstractChainingTable(Function<V, K> getKey) {
    this.slots = this.initSlots(INIT_NUM_SLOTS, getKey);
    this.getKey = getKey;
    this.numValues = 0;
  }

  public V get(K key) {
    int hash = this.getHash(key);
    AssocList<K, V> chain = this.slots[hash];

    return chain.get(key);
  }

  public void set(V value) {
    K key = this.getKey.apply(value);
    int hash = this.getHash(key);

    this.updateChain(hash, chain -> chain.set(value));
  }

  public void remove(K key) {
    int hash = this.getHash(key);

    this.updateChain(hash, chain -> chain.remove(key));
  }

  public int getSize() {
    return this.numValues;
  }

  public Iterable<V> getValues() {
    CircularLinkedList<V> list = new CircularLinkedList<>();

    for (int i = 0; i < this.slots.length; ++i) {
      for (V value : this.slots[i].getValues()) {
        list.prepend(value);
      }
    }

    return list;
  }

  protected void updateChain(int hash, Consumer<AssocList<K, V>> updateFunc) {
    AssocList<K, V> chain = this.slots[hash];
    int oldSize = chain.getSize();

    updateFunc.accept(chain);

    int deltaSize = chain.getSize() - oldSize;
    this.numValues += deltaSize;
  }

  protected AssocList<K, V>[] initSlots(int numSlots, Function<V, K> getKey) {
    @SuppressWarnings("unchecked")
    AssocList<K, V>[] slots = new AssocList[numSlots];

    for (int i = 0; i < slots.length; ++i) {
      slots[i] = new AssocList<K, V>(getKey);
    }

    return slots;
  }

  abstract protected int getHash(K key);
}
package org.jal.collections.map;

import org.jal.collections.dictionary.Dictionary;
import org.jal.collections.dictionary.DoubleHashingTable;
import org.jal.util.Pair;

public class DoubleHashingMap<K, V> implements Map<K, V> {
  private Dictionary<K, Pair<K, V>> dict;

  public DoubleHashingMap() {
    this.dict = new DoubleHashingTable<>(Pair::getFirst);
  }

  public void set(K key, V value) {
    Pair<K, V> pair = new Pair<>(key, value);
    this.dict.set(pair);
  }

  public V get(K key) {
    Pair<K, V> pair = this.dict.get(key);
    if (pair == null) {
      return null;
    }

    V value = pair.getSecond();
    return value;
  }

  public void remove(K key) {
    this.dict.remove(key);
  }

  public int getSize() {
    return this.dict.getSize();
  }

  public Iterable<Pair<K, V>> getEntries() {
    return this.dict.getValues();
  }
}
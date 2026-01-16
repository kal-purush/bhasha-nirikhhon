package org.jal.collections.dictionary;

import java.util.function.Function;

public class ChainingTable<K, V> extends AbstractChainingTable<K, V> {
  private static double MAX_LOAD_FACTOR = 4.0;
  private static double MIN_LOAD_FACTOR = 1.0;
  private static double GROWTH_FACTOR = 2.0;

  public ChainingTable(Function<V, K> getKey) {
    super(getKey);
  }

  @Override
  public void set(V value) {
    super.set(value);

    if (this.isTooManyLoaded()) {
      int extendedSize = (int)(this.slots.length * GROWTH_FACTOR);
      this.resizeSlots(extendedSize);
    }
  }

  @Override
  public void remove(K key) {
    super.remove(key);

    if (this.isTooFewLoaded()) {
      int reducedSize = (int)(this.slots.length / GROWTH_FACTOR);
      this.resizeSlots(reducedSize);
    }
  }

  @Override
  protected int getHash(K key) {
    return key.hashCode() % this.slots.length;
  }

  private void resizeSlots(int newSize) {
    AssocList<K, V>[] oldSlots = this.slots;

    AssocList<K, V>[] newSlots = this.initSlots(newSize, this.getKey);
    this.slots = newSlots;

    // rehash values
    for (int i = 0; i < oldSlots.length; ++i) {
      AssocList<K, V> oldChain = oldSlots[i];
      for (V values : oldChain.getValues()) {
        int hash = this.getHash(this.getKey.apply(values));
        this.slots[hash].set(values);
      }
    }
  }

  private boolean isTooManyLoaded() {
    return this.getLoadFactor() >= MAX_LOAD_FACTOR;
  }

  private boolean isTooFewLoaded() {
    if (this.slots.length <= INIT_NUM_SLOTS) {
      return false;
    }

    return this.getLoadFactor() <= MIN_LOAD_FACTOR;
  }

  private double getLoadFactor() {
    return (double)this.numValues / (double)this.slots.length;
  }
}
package org.jal.collections.dictionary;

import java.util.function.Function;

import org.jal.collections.list.CircularLinkedList;

abstract class AbstractOpenAddressingTable<K, V> implements Dictionary<K, V> {
  protected static int INIT_NUM_SLOTS = 16;
  protected static String EXCEPTION_FULL_MESSAGE = "Hash table is full";

  private static double MAX_LOAD_FACTOR = 0.750;
  private static double MIN_LOAD_FACTOR = 0.125;
  private static double GROWTH_FACTOR = 2.0;

  protected Slot<V>[] slots;
  protected Function<V, K> getKey;
  protected int numValues;
  protected int numRemoved;

  public AbstractOpenAddressingTable(Function<V, K> getKey) {
    this.slots = this.initSlots(INIT_NUM_SLOTS);
    this.getKey = getKey;
    this.numValues = 0;
    this.numRemoved = 0;
  }

  public V get(K key) {
    return this.getValue(key);
  }

  public double l() {
    return this.getLoadFactor();
  }

  public void set(V value) {
    this.setValue(value);

    if (this.isTooManyLoaded()) {
      int extendedSize = (int)(this.slots.length * GROWTH_FACTOR);
      this.resizeSlots(extendedSize);
    }
  }

  public void remove(K key) {
    this.removeValue(key);

    if (this.isTooFewLoaded()) {
      int reduced = (int)(this.slots.length / GROWTH_FACTOR);
      this.resizeSlots(reduced);
    }
  }

  public int getSize() {
    return this.numValues;
  }

  public Iterable<V> getValues() {
    CircularLinkedList<V> list = new CircularLinkedList<>();

    for (int i = 0; i < this.slots.length; ++i) {
      Slot<V> slot = this.slots[i];
      if (slot == null || slot.isRemoved()) {
        continue;
      }

      V value = this.slots[i].getValue();
      list.prepend(value);
    }

    return list;
  }

  private V getValue(K key) {
    Iterable<Integer> probeSeq = this.getProbeSequence(key);

    for (int i : probeSeq) {
      Slot<V> slot = this.slots[i];

      if (slot == null) {
        return null;
      }

      if (slot.isRemoved()) {
        continue;
      }

      if (this.hasEqualKey(slot, key)) {
        return this.slots[i].getValue();
      }
    }

    throw new IllegalStateException(EXCEPTION_FULL_MESSAGE);
  }

  private void setValue(V value) {
    K key = this.getKey.apply(value);
    Iterable<Integer> probeSeq = this.getProbeSequence(key);

    for (int i : probeSeq) {
      Slot<V> slot = this.slots[i];

      if (slot == null) {
        this.slots[i] = new Slot<V>(value);
        this.numValues++;
        return;
      }

      if (slot.isRemoved()) {
        continue;
      }

      if (this.hasEqualKey(slot, key)) {
        this.slots[i] = new Slot<V>(value);
        return;
      }
    }

    throw new IllegalStateException(EXCEPTION_FULL_MESSAGE);
  }

  private void removeValue(K key) {
    Iterable<Integer> probeSeq = this.getProbeSequence(key);

    for (int i : probeSeq) {
      Slot<V> slot = this.slots[i];

      if (slot == null) {
        return;
      }

      if (slot.isRemoved()) {
        continue;
      }

      if (this.hasEqualKey(slot, key)) {
        this.slots[i].remove();
        this.numValues--;
        this.numRemoved++;
        return;
      }
    }

    throw new IllegalStateException(EXCEPTION_FULL_MESSAGE);
  }

  abstract protected Iterable<Integer> getProbeSequence(K key);

  protected Slot<V>[] initSlots(int numSlots) {
    @SuppressWarnings("unchecked")
    Slot<V>[] slots = new Slot[numSlots];

    return slots;
  }

  private boolean hasEqualKey(Slot<V> slot, K key) {
    K slotKey = this.getKey.apply(slot.getValue());

    return key.equals(slotKey);
  }

  private void resizeSlots(int newSize) {
    Slot<V>[] oldSlots = this.slots;
    this.slots = this.initSlots(newSize);

    this.numValues = 0;
    this.numRemoved = 0;

    // rehash values
    for (int i = 0; i < oldSlots.length; ++i) {
      Slot<V> oldSlot = oldSlots[i];

      // skip if no value
      if (oldSlot == null || oldSlot.isRemoved()) {
        continue;
      }

      V value = oldSlots[i].getValue();
      this.setValue(value);
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
    int numOccupied = this.numValues + this.numRemoved;

    return (double)numOccupied / (double)this.slots.length;
  }
}
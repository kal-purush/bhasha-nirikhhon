package org.jal.collections.dictionary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

abstract class CommonTest<K, V> {
  abstract protected Dictionary<K, V> createDictionary();
  abstract protected ArrayList<V> generateValues();
  abstract protected ArrayList<K> generateKeys();

  @DisplayName("set() should set items")
  @Test
  public void testSet() {
    Dictionary<K, V> dict = this.createDictionary();
    ArrayList<K> keys = this.generateKeys();
    ArrayList<V> values = this.generateValues();

    for (V value : values) {
      dict.set(value);
    }

    for (int i = 0; i < keys.size(); ++i) {
      K key = keys.get(i);
      V value = values.get(i);

      assertEquals(value, dict.get(key));
    }
  }

  @DisplayName("remove() should remove items")
  @Test
  public void testRemove() {
    Dictionary<K, V> dict = this.createDictionary();
    ArrayList<K> keys = this.generateKeys();
    ArrayList<V> values = this.generateValues();

    for (V value : values) {
      dict.set(value);
    }
    for (K key : keys) {
      dict.remove(key);
    }

    for (K key : keys) {
      assertEquals(null, dict.get(key));
    }
  }

  @DisplayName("getSize() should get the size")
  @Test
  public void testGetSize() {
    Dictionary<K, V> dict = this.createDictionary();
    ArrayList<V> values = this.generateValues();

    for (V value : values) {
      dict.set(value);
    }

    assertEquals(values.size(), dict.getSize());
  }

  @DisplayName("getSize() should get zero if all removed")
  @Test
  public void testGetSizeRemoved() {
    Dictionary<K, V> dict = this.createDictionary();
    ArrayList<K> keys = this.generateKeys();
    ArrayList<V> values = this.generateValues();

    for (V value : values) {
      dict.set(value);
    }
    for (K key : keys) {
      dict.remove(key);
    }

    assertEquals(0, dict.getSize());
  }

  @DisplayName("should be iterable")
  @Test
  public void testIterable() {
    Dictionary<K, V> dict = this.createDictionary();
    ArrayList<V> values = this.generateValues();

    for (V value : values) {
      dict.set(value);
    }

    Iterable<V> dictValues = dict.getValues();
    int dictValueSize = StreamSupport.stream(dictValues.spliterator(), false).toArray().length;
    assertEquals(values.size(), dictValueSize);
  }
}

class SingleHashInt {
  private int value;

  public SingleHashInt(int value) {
    this.value = value;
  }

  public int get() {
    return this.value;
  }

  @Override
  public int hashCode() {
    return 0; // single hash
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SingleHashInt)) {
      return false;
    }

    SingleHashInt that = (SingleHashInt) obj;
    return this.get() == that.get();
  }
}

class DictFactory {
  private static Function<Integer, Integer> getIntDictKey = v -> v;
  private static Function<Integer, SingleHashInt> getSingleHashIntDictKey = v -> new SingleHashInt(v);

  public static Dictionary<Integer, Integer> createFixedChainingIntDict() {
    return new FixedChainingTable<Integer, Integer>(getIntDictKey);
  }

  public static Dictionary<Integer, Integer> createChainingIntDict() {
    return new ChainingTable<Integer, Integer>(getIntDictKey);
  }

  public static Dictionary<Integer, Integer> createLinearProbingIntDict() {
    return new LinearProbingTable<Integer, Integer>(getIntDictKey);
  }

  public static Dictionary<Integer, Integer> createDoubleHashingIntDict() {
    return new DoubleHashingTable<Integer, Integer>(getIntDictKey);
  }

  public static Dictionary<SingleHashInt, Integer> createFixedChainingSingleHashIntDict() {
    return new FixedChainingTable<SingleHashInt, Integer>(getSingleHashIntDictKey);
  }

  public static Dictionary<SingleHashInt, Integer> createChainingSingleHashIntDict() {
    return new ChainingTable<SingleHashInt, Integer>(getSingleHashIntDictKey);
  }

  public static Dictionary<SingleHashInt, Integer> createLinearProbingSingleHashIntDict() {
    return new LinearProbingTable<SingleHashInt, Integer>(getSingleHashIntDictKey);
  }

  public static Dictionary<SingleHashInt, Integer> createDoubleHashingSingleHashIntDict() {
    return new DoubleHashingTable<SingleHashInt, Integer>(getSingleHashIntDictKey);
  }
}

public class DictionaryTest {
  @DisplayName("Integer to integer")
  @Nested
  class IntTest {
    static int SIZE = 300;

    static ArrayList<Integer> generateInts(int size) {
      return Stream.iterate(0, i->i+1).limit(300).collect(Collectors.toCollection(ArrayList::new));
    }

    @DisplayName("FixedChainingTable class")
    @Nested
    class FixedChainingTest extends CommonTest<Integer, Integer> {
      @Override
      protected Dictionary<Integer, Integer> createDictionary() {
        return DictFactory.createFixedChainingIntDict();
      }

      @Override
      protected ArrayList<Integer> generateKeys() {
        return generateInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("ChainingTable class")
    @Nested
    class ChainingTest extends CommonTest<Integer, Integer> {
      @Override
      protected Dictionary<Integer, Integer> createDictionary() {
        return DictFactory.createChainingIntDict();
      }

      @Override
      protected ArrayList<Integer> generateKeys() {
        return generateInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("LinearProbingTable class")
    @Nested
    class LinearProbingTest extends CommonTest<Integer, Integer> {
      @Override
      protected Dictionary<Integer, Integer> createDictionary() {
        return DictFactory.createLinearProbingIntDict();
      }

      @Override
      protected ArrayList<Integer> generateKeys() {
        return generateInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("ChainingTable class")
    @Nested
    class DoubleHashingTest extends CommonTest<Integer, Integer> {
      @Override
      protected Dictionary<Integer, Integer> createDictionary() {
        return DictFactory.createDoubleHashingIntDict();
      }

      @Override
      protected ArrayList<Integer> generateKeys() {
        return generateInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };
  }

  @DisplayName("Single hash integer to integer")
  @Nested
  class SingleHashIntTest {
    static int SIZE = 300;

    static ArrayList<Integer> generateInts(int size) {
      return Stream.iterate(0, i->i+1).limit(300).collect(Collectors.toCollection(ArrayList::new));
    }

    static ArrayList<SingleHashInt> generateSingleHashInts(int size) {
      return Stream.iterate(0, i->i+1).map(i -> new SingleHashInt(i)).limit(300).collect(Collectors.toCollection(ArrayList::new));
    }

    @DisplayName("FixedChainingTable class")
    @Nested
    class FixedChainingTest extends CommonTest<SingleHashInt, Integer> {
      @Override
      protected Dictionary<SingleHashInt, Integer> createDictionary() {
        return DictFactory.createFixedChainingSingleHashIntDict();
      }

      @Override
      protected ArrayList<SingleHashInt> generateKeys() {
        return generateSingleHashInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("ChainingTable class")
    @Nested
    class ChainingTest extends CommonTest<SingleHashInt, Integer> {
      @Override
      protected Dictionary<SingleHashInt, Integer> createDictionary() {
        return DictFactory.createChainingSingleHashIntDict();
      }

      @Override
      protected ArrayList<SingleHashInt> generateKeys() {
        return generateSingleHashInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("LinearProbingTable class")
    @Nested
    class LinearProbingTest extends CommonTest<SingleHashInt, Integer> {
      @Override
      protected Dictionary<SingleHashInt, Integer> createDictionary() {
        return DictFactory.createLinearProbingSingleHashIntDict();
      }

      @Override
      protected ArrayList<SingleHashInt> generateKeys() {
        return generateSingleHashInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };

    @DisplayName("ChainingTable class")
    @Nested
    class DoubleHashingTest extends CommonTest<SingleHashInt, Integer> {
      @Override
      protected Dictionary<SingleHashInt, Integer> createDictionary() {
        return DictFactory.createDoubleHashingSingleHashIntDict();
      }

      @Override
      protected ArrayList<SingleHashInt> generateKeys() {
        return generateSingleHashInts(SIZE);
      }

      @Override
      protected ArrayList<Integer> generateValues() {
        return generateInts(SIZE);
      }
    };
  }
}
package org.jal.search;

import static org.jal.search.BinarySearchArray.search;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class BinarySearchArrayTestExample {
  static Integer[] SORTED = { 9, 26, 43, 84, 93, 110 };
  static Comparator<Integer> REM8 = Comparator.comparing(v -> v % 8);

  @Nested
  class Rem8Ordering {
    @Test
    public void testFound() {
      Integer[] arr = SORTED;
      Integer valueAt2 = arr[2];

      int index = search(arr, valueAt2, REM8);

      assertEquals(2, index);
    }
  }

  @Nested
  class DefaultComparator {
    @Test
    public void testFound() {
      Integer[] arr = SORTED;
      Integer valueAt2 = arr[2];

      int index = search(arr, valueAt2);

      assertEquals(2, index);
    }
  }
}
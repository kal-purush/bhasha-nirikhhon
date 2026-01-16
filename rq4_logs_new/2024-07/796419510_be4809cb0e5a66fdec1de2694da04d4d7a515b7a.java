package org.jal.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class HashesTest {
  @DisplayName("multiplyShiftHash() should be less than the bound")
  @Test
  public void testSwap() {
    int bound = 100;

    for (long i = 0; i < 10; ++i) {
      assertTrue(Hashes.multShiftHash(i, bound) < bound);
    }
    for (long i = 0x7FFFFFF0; i < 0x7FFFFFFF; ++i) {
      assertTrue(Hashes.multShiftHash(i, bound) < bound);
    }
  }
}
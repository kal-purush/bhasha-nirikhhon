package org.jal.collections.dictionary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class LinearProbingTableTestExample {
  @Test
  public void testAttendance() {
    LinearProbingTable<String, String> table = new LinearProbingTable<>(v -> v);

    table.set("John");
    table.set("Jane");

    assertNotEquals(null, table.get("John"));
    assertNotEquals(null, table.get("Jane"));
    assertEquals(null, table.get("Tom"));

    table.set("Tom");

    assertNotEquals(null, table.get("John"));
    assertNotEquals(null, table.get("Jane"));
    assertNotEquals(null, table.get("Tom"));

    table.remove("John");

    assertEquals(null, table.get("John"));
    assertNotEquals(null, table.get("Jane"));
    assertNotEquals(null, table.get("Tom"));
  }
}
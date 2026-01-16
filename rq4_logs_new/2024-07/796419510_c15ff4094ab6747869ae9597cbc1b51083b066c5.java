package org.jal.collections.stack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

public class ArrayStackTestExample {
  @Test
  public void testPushAndPop() {
    Stack<Integer> stack = new ArrayStack<>();
    stack.push(42);
    stack.push(43);

    assertEquals(43, stack.pop());
  }

  @Test
  public void testPopForEmptyStack() {
    Stack<Integer> stack = new ArrayStack<>();

    assertThrows(NoSuchElementException.class, () -> stack.pop());
  }
}
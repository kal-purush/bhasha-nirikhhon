package com.auberer.compilerdesignlectureproject.lexer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StringLiteralStateMachineTest {

  @Test
  @DisplayName("Happy path")
  public void testHappyPath() {
    String input = "\"string\"";
    StringLiteralStateMachine stateMachine = new StringLiteralStateMachine();
    stateMachine.init();
    stateMachine.reset();
    for (char c : input.toCharArray()) {
      assertFalse(stateMachine.isInAcceptState());
      assertDoesNotThrow(() -> stateMachine.processInput(c));
    }
    assertTrue(stateMachine.isInAcceptState());
  }

  @Test
  @DisplayName("Test erroneous input")
  public void testErroneousInput() {
    String input = "\"string"; // Closing quote is missing
    StringLiteralStateMachine stateMachine = new StringLiteralStateMachine();
    stateMachine.init();
    stateMachine.reset();
    for (char c : input.toCharArray()) {
      assertFalse(stateMachine.isInAcceptState());
      assertDoesNotThrow(() -> stateMachine.processInput(c));
    }
    assertFalse(stateMachine.isInAcceptState());
  }

}
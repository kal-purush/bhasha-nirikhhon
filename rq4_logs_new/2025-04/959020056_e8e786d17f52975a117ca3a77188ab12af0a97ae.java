package com.auberer.compilerdesignlectureproject.lexer;

import com.auberer.compilerdesignlectureproject.lexer.statemachine.StateMachine;
import com.auberer.compilerdesignlectureproject.reader.CodeLoc;
import com.auberer.compilerdesignlectureproject.reader.Reader;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.misc.Pair;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class Lexer implements ILexer {

  private final Reader reader;
  private final List<StateMachine> stateMachines = new ArrayList<StateMachine>();
  private final Queue<Pair<Character, CodeLoc>> inputBuffer = new LinkedList<Pair<Character, CodeLoc>>();
  private Token currentToken;
  private final boolean dumpTokens;

  public Lexer(Reader reader, boolean dumpTokens) {
    this.reader = reader;
    this.dumpTokens = dumpTokens;

    // Here, the order matters. The last state machine has the highest priority in case
    // multiple machines match the given input at the same length.
    stateMachines.add(new StringLiteralStateMachine());
  }

  @Override
  public Token getToken() {
    return currentToken;
  }

  private char peekChar() {
    if (!inputBuffer.isEmpty()) {
      return inputBuffer.peek().a;
    }
    return reader.getChar();
  }

  private Pair<Character, CodeLoc> getCurrentCharAndCodeLoc() {
    if (!inputBuffer.isEmpty()) {
      return inputBuffer.poll();
    }
    char currentChar = reader.getChar();
    CodeLoc currentCodeLoc = reader.getCodeLoc().clone();
    reader.advance();
    return new Pair<>(currentChar, currentCodeLoc);
  }

  @Override
  public void advance() {
    // Reset all state machines to start from the respective initial state
    for (StateMachine stateMachine : stateMachines) {
      stateMachine.reset();
    }

    // Skip any whitespaces
    while (!(reader.isEOF() && inputBuffer.isEmpty()) && Character.isWhitespace(peekChar())) {
      getCurrentCharAndCodeLoc();
    }

    CodeLoc tokenCodeLoc = null;

    // Run all state machines in parallel on the given char input stream
    List<StateMachine> runningMachines = new ArrayList<>(stateMachines);
    Map<StateMachine, Integer> acceptingMachines = new LinkedHashMap<>();
    Queue<Pair<Character, CodeLoc>> newInputBuffer = new LinkedList<>();
    while (!(reader.isEOF() && inputBuffer.isEmpty()) && !runningMachines.isEmpty()) {
      Pair<Character, CodeLoc> curCharAndCodeLoc = getCurrentCharAndCodeLoc();
      newInputBuffer.add(curCharAndCodeLoc);
      if (tokenCodeLoc == null)
        tokenCodeLoc = curCharAndCodeLoc.b;

      for (StateMachine stateMachine : new CopyOnWriteArrayList<>(runningMachines)) {
        // Try to process the input. If the processing throws an exception, the machine is in an invalid state
        // and should be removed from the list of running machines.
        try {
          stateMachine.processInput(curCharAndCodeLoc.a);
        } catch (IllegalStateException e) {
          String currentInput = stateMachine.getAcceptedInput() + curCharAndCodeLoc.a;
          log.debug("State machine does not match input {}: {}", currentInput, e.getMessage());
          runningMachines.remove(stateMachine);
          continue;
        }

        // If the machine is in an accept state, add it to the list of accepting machines
        if (stateMachine.isInAcceptState()) {
          acceptingMachines.remove(stateMachine);
          acceptingMachines.put(stateMachine, stateMachine.getAcceptedInput().length());
          // Clear input buffer to make sure we backtrack to this point in the input in case
          // no other running machine accepts later.
          newInputBuffer.clear();
        }
      }
    }

    // Add the remaining characters to the input buffer
    inputBuffer.addAll(newInputBuffer);

    // If EOF is reached, finalize the token
    if (acceptingMachines.isEmpty()) {
      currentToken = new Token(TokenType.TOK_INVALID, "", tokenCodeLoc);
      return;
    }

    // Check which of the running machines has the highest priority and set the current token accordingly
    Map.Entry<StateMachine, Integer> winningEntry = null;
    for (Map.Entry<StateMachine, Integer> entry : acceptingMachines.entrySet())
      if (winningEntry == null || entry.getValue().compareTo(winningEntry.getValue()) > 0)
        winningEntry = entry;
    StateMachine winningMachine = winningEntry.getKey();
    currentToken = new Token(winningMachine.getTokenType(), winningMachine.getAcceptedInput(), tokenCodeLoc);
    if (dumpTokens)
      System.out.println(currentToken);
  }

  @Override
  public void expect(TokenType expectedType) throws RuntimeException {
    if (currentToken.getType() != expectedType) {
      throw new RuntimeException("Unexpected token type: " + currentToken.getType());
    }
    advance();
  }

  @Override
  public void expectOneOf(Set<TokenType> expectedTypes) {
    if (expectedTypes.contains(currentToken.getType())) {
      throw new RuntimeException("Unexpected token type: " + currentToken.getType());
    }
    advance();
  }

  @Override
  public boolean isEOF() {
    return reader.isEOF();
  }

  @Override
  public CodeLoc getCodeLoc() {
    return reader.getCodeLoc();
  }
}
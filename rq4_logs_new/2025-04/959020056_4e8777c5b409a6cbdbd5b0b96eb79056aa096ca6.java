package com.auberer.compilerdesignlectureproject.lexer;

import com.auberer.compilerdesignlectureproject.lexer.statemachine.Range;
import com.auberer.compilerdesignlectureproject.lexer.statemachine.State;
import com.auberer.compilerdesignlectureproject.lexer.statemachine.StateMachine;

public class DoubleLiteralStateMachine extends StateMachine {
  @Override
  public void init() {
    // States
    State stateStart = new State("Start");
    stateStart.setStartState(true);
    addState(stateStart);

    State stateZero = new State("Zero");
    addState(stateZero);


    State stateBeforeDot = new State("Before");
    addState(stateBeforeDot);
    State stateDot = new State("Dot");
    addState(stateDot);

    State stateEnd = new State("Double");
    stateEnd.setAcceptState(true);
    addState(stateEnd);

    // Transitions

    addCharTransition(stateStart, stateZero, '0');
    addRangeTransition(stateStart, stateBeforeDot, new Range('1', '9'));

    addRangeTransition(stateBeforeDot, stateBeforeDot, new Range('0', '9'));
    addCharTransition(stateBeforeDot, stateDot,'.');
    addCharTransition(stateZero, stateDot,'.');

    addRangeTransition(stateDot, stateEnd, new Range('0', '9'));
    addRangeTransition(stateEnd, stateEnd, new Range('0', '9'));
  }

  @Override
  public TokenType getTokenType() {
    return TokenType.TOK_DOUBLE_LIT;
  }
}
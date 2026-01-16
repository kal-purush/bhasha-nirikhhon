package com.naosim.washlet.eventpattern;

import com.naosim.washlet.common.State;

public interface Context {
    State getState();
    void updateState(State state);
}
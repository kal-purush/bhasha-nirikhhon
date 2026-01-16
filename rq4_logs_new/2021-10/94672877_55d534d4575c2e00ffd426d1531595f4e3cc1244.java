package com.kuldeep.design.callcenter;

import java.time.Duration;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
class Call {
    private final int id;
    private CallState state = CallState.WAITING;
    private final Duration duration = Duration.ofSeconds(5);

    private Call(int id) {
        this.id = id;
    }

    public static Call of(int id) {
        return new Call(id);
    }

    public Duration duration() {
        return duration;
    }

    public int getId() {
        return id;
    }

    public CallState getState() {
        return state;
    }

    public void changeTo(CallState newState) {
        this.state = newState;
    }
}
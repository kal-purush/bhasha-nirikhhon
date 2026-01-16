package com.teamness.smane.controller;

import com.teamness.smane.event.BuzzerEvent;
import com.teamness.smane.event.CaneEvents;
import com.teamness.smane.interfaces.IDirectionOutput;

/**
 * Created by mac15001900 on 3/3/18.
 */

public class TemporaryBuzzerThingy implements IDirectionOutput {

    private final double DEFAULT_LENGTH = 0.2;

    @Override
    public void giveDirection(double angle, double strength) {
        CaneEvents.BT_OUT.trigger(new BuzzerEvent(angle, strength, DEFAULT_LENGTH));
    }
}
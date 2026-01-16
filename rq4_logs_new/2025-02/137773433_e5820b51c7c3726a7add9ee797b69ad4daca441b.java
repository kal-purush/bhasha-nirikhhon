package com.jcoadyschaebitz.neon.input;

import com.badlogic.gdx.controllers.Controller;

public interface RumbleGamepad extends Controller {
    boolean rumble(float leftMagnitude, float rightMagnitude, int duration_ms);
}
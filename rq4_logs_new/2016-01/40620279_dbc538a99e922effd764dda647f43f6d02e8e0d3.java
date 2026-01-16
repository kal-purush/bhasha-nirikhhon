package com.github.st1hy.sabre.image.gdx.touch;

import android.support.annotation.NonNull;

import com.github.st1hy.gesturedetector.GestureDetector;
import com.github.st1hy.view.ForwardingOnTouchListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Filters out events that start very close to the top and bottom borders.
 */
public class DeadZoneOnTouchListener extends ForwardingOnTouchListener<GestureDetector> implements GestureDetector {

    public DeadZoneOnTouchListener(@NonNull GestureDetector delegate) {
        super(delegate, setupPreconditions());
    }

    private static List<TouchPredicate> setupPreconditions() {
        List<TouchPredicate> list = new ArrayList<>(1);
        list.add(new DeadZoneTouchPredicate());
        return list;
    }

    @Override
    public void invalidate() {
        delegate().invalidate();
    }

}
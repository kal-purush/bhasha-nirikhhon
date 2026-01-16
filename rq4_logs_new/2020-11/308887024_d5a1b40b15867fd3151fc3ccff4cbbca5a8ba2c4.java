package com.marketplace.framework;

import com.marketplace.framework.InvalidEventException;

import java.util.ArrayList;
import java.util.List;

public abstract class Entity<T> {
    private List<Object> events;

    public Entity() {
        events = new ArrayList<>();
    }

    public void apply(Object event) {
        ensureValidState(event);
        events.add(event);
    }

    public abstract void ensureValidState(Object event);

    public void clearChanges() {
        events.clear();
    }

    public List<Object> getChanges() {
        return events;
    }

    public void when() {

    }
}
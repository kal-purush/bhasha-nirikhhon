package com.github.jmcampanini.hrm.emulator.impl;

import com.github.jmcampanini.hrm.emulator.Outbox;
import com.github.jmcampanini.hrm.emulator.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The default implementation of {@link Outbox}.
 */
public class DefaultOutbox implements Outbox {

    private final List<Value> data;

    public DefaultOutbox() {
        this.data = Lists.newArrayList();
    }

    @Override
    public void put(Value value) {
        checkArgument(value != null, "The value cannot be null.");
        this.data.add(value);
    }

    @Override
    public List<Value> data() {
        return ImmutableList.copyOf(this.data);
    }
}
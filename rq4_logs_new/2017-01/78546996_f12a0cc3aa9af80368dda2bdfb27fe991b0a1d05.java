package com.github.jmcampanini.hrm.emulator.levels;

import com.github.jmcampanini.hrm.emulator.*;
import com.github.jmcampanini.hrm.emulator.impl.FixedLengthInbox;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Abstract class that provides helper functions for other level test suites.
 */
public abstract class AbstractLevel {

    private ImmutableCpu.Builder cpuBuilder = ImmutableCpu.builder();

    protected void setFloor(Floor floor) {
        this.cpuBuilder.floor(floor);
    }

    protected void assertOutputMatches(
            List<Command> program,
            ImmutableList<String> input,
            ImmutableList<String> output) {

        Inbox inbox = FixedLengthInbox.withValues(input.stream()
                .map(Thing::of)
                .collect(Collectors.toList()));

        Cpu cpu = this.cpuBuilder
                .inbox(inbox)
                .build();

        cpu.processor().run(cpu, program);

        List<String> data = cpu.outbox()
                .data().stream()
                .map(Thing::value)
                .collect(Collectors.toList());

        assertThat(data, equalTo(output));
    }
}
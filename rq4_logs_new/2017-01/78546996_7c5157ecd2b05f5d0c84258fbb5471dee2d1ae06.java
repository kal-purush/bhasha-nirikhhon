package com.github.jmcampanini.hrm.emulator.impl;

import com.github.jmcampanini.hrm.emulator.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@link Processor}.
 */
public class DefaultProcessor implements Processor {

    private final Inbox inbox;
    private final Outbox outbox;
    private final Worker worker;
    private final ProgramCounter programCounter;
    private final AtomicLong steps;

    public DefaultProcessor(Inbox inbox, Outbox outbox, Worker worker, ProgramCounter programCounter) {
        this.inbox = checkNotNull(inbox);
        this.outbox = checkNotNull(outbox);
        this.worker = checkNotNull(worker);
        this.programCounter = checkNotNull(programCounter);
        this.steps = new AtomicLong(0);
    }

    @Override
    public Inbox inbox() {
        return this.inbox;
    }

    @Override
    public Outbox outbox() {
        return this.outbox;
    }

    @Override
    public Worker worker() {
        return this.worker;
    }

    @Override
    public ProgramCounter programCounter() {
        return this.programCounter;
    }

    @Override
    public long steps() {
        return this.steps.get();
    }

    @Override
    public void run(List<Command> program) {
        try {
            int endOfProgram = program.size();

            while (this.programCounter().get() < endOfProgram) {
                int nextCommandIndex = this.programCounter.get();
                Command nextCommand = program.get(nextCommandIndex);
                nextCommand.execute(this);
            }

        } catch (ProgramEndException e) {
            // TODO: handle this exception better
            throw new RuntimeException(e);
        }
    }
}
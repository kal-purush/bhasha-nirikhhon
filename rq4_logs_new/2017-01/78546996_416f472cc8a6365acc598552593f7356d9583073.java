package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.Command;
import com.github.jmcampanini.hrm.emulator.Processor;
import com.github.jmcampanini.hrm.emulator.ProgramEndException;
import com.github.jmcampanini.hrm.emulator.Thing;
import org.immutables.value.Value;

/**
 * The {@link Command} to take input from the {@link com.github.jmcampanini.hrm.emulator.Inbox}.
 */
@Value.Immutable
public interface InboxCommand extends Command {

    InboxCommand INSTANCE = ImmutableInboxCommand.builder().build();

    @Override
    default void execute(Processor processor) throws ProgramEndException {
        Thing thing = processor.inbox().take();
        processor.worker().setThing(thing);
        processor.programCounter().increment();
    }
}
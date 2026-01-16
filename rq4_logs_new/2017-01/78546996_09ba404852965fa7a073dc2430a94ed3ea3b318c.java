package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.Command;
import com.github.jmcampanini.hrm.emulator.Processor;
import com.github.jmcampanini.hrm.emulator.ProgramEndSignal;
import org.immutables.value.Value;

/**
 * Do nothing, used mostly for testing.
 */
@Value.Immutable
public interface NoopCommand extends Command {

    NoopCommand INSTANCE = ImmutableNoopCommand.builder().build();

    @Override
    default void execute(Processor processor) throws ProgramEndSignal {
        processor.programCounter().increment();
    }
}
package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.Command;
import com.github.jmcampanini.hrm.emulator.Processor;
import com.github.jmcampanini.hrm.emulator.ProgramEndSignal;
import org.immutables.value.Value;

@Value.Immutable
public interface JumpCommand extends Command {

    int index();

    static JumpCommand jumpTo(int index) {
        return ImmutableJumpCommand.builder()
                .index(index)
                .build();
    }

    @Override
    default void execute(Processor processor) throws ProgramEndSignal {
        processor.programCounter().setToIndex(index());
    }
}
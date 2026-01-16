package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.*;
import org.immutables.value.Value;

/**
 * The {@link Command} to copy from the {@link com.github.jmcampanini.hrm.emulator.Floor}.
 */
@Value.Immutable
public interface CopyFromCommand extends Command {

    int tileNum();

    static CopyFromCommand fromTile(int tileNum) {
        return ImmutableCopyFromCommand.builder()
                .tileNum(tileNum)
                .build();
    }

    @Override
    default void execute(Cpu cpu, Pointer cmdPointer) throws ProgramEndSignal {
        Thing thing = cpu.floor().get(tileNum())
                .orElseThrow(() -> new ProcessorException("No value in floor tile: " + tileNum()));

        cpu.worker().setThing(thing);
        cmdPointer.increment();
    }
}
package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.*;
import org.immutables.value.Value;

/**
 * The {@link Command} to copy to the {@link com.github.jmcampanini.hrm.emulator.Floor}.
 */
@Value.Immutable
public interface CopyToCommand extends Command {

    int tileNum();

    static CopyToCommand toTile(int tileNum) {
        return ImmutableCopyToCommand.builder()
                .tileNum(tileNum)
                .build();
    }

    @Override
    default void execute(Cpu cpu, Pointer cmdPointer) throws ProgramEndSignal {
        Thing thing = cpu.worker().thing()
                .orElseThrow(() -> new ProcessorException("Worker has no thing"));

        cpu.floor().set(tileNum(), thing);
        cmdPointer.increment();
    }
}
package com.github.jmcampanini.hrm.emulator.commands;

import com.github.jmcampanini.hrm.emulator.*;
import org.immutables.value.Value;

/**
 * The {@link Command} to add the contents of a tile on the {@link com.github.jmcampanini.hrm.emulator.Floor} to
 * whatever is being held by the {@link com.github.jmcampanini.hrm.emulator.Worker}.
 */
@Value.Immutable
public interface AddCommand extends Command {

    int tileNum();

    static AddCommand withTile(int tileNum) {
        return ImmutableAddCommand.builder()
                .tileNum(tileNum)
                .build();
    }

    @Override
    default void execute(Cpu cpu, Pointer cmdPointer) throws ProgramEndSignal {
        if (!cpu.floor().get(tileNum()).isPresent()) {
            throw new TileEmptyException(tileNum());
        }

        if (!cpu.worker().thing().isPresent()) {
            throw new WorkerEmptyException();
        }

        Thing floorThing = cpu.floor().get(tileNum()).get();
        Thing workerThing = cpu.worker().thing().get();
        Thing added = workerThing.add(floorThing);

        cpu.worker().setThing(added);
        cmdPointer.increment();
    }
}
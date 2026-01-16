package dev.vality.schedulator.handler.machinegun.event;

import dev.vality.damsel.schedule.ScheduleChange;
import dev.vality.machinarium.domain.SignalResultData;
import dev.vality.machinarium.domain.TMachine;
import dev.vality.machinarium.domain.TMachineEvent;

public interface MachineEventHandler {
    SignalResultData<ScheduleChange> handleEvent(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event);

    boolean isHandle(TMachine<ScheduleChange> machine, TMachineEvent<ScheduleChange> event);
}
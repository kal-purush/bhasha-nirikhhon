import { ReduxAction } from "../interfaces";
import { Sequence, Color } from "../sequences/interfaces";

/** A sequence of "sequences" scheduled after a starting point. */
export interface Regimen {
    _id?: string;
    /** Friendly identifier for humans to easily identify regimens. */
    name: string;
    color: Color;
    sequenceSchedules: SequenceSchedule[];
};

/** Individual step that a regimen will execute at a point in time. */
export interface SequenceSchedule {
    sequence: Sequence;
    /** Time (in milliseconds?) to wait before executing the sequence */
    timeFromEpoch: number;
};

/** How Regimen state is stored in the application. Used by Regimen reducer mostly */
export interface RegimensState {
    current: number;
    all: Regimen[];
}

export interface RegimensActionHandler {
    [actionName: string]: (state: RegimensState,
        action: ReduxAction<any>) => RegimensState;
}
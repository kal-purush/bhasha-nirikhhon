import { Action } from 'redux';

export const SET_INTERVALS = 'SET_WORKOUT_CREATOR_INTERVALS';
export const UNDO = 'WORKOUT_CREATOR_UNDO';
export const REDO = 'WORKOUT_CREATOR_REDO';

export type Interval = {
	intensity: number;
	length: number;
	color: string;
};

export interface SetIntervalsAction extends Action {
	type: 'SET_WORKOUT_CREATOR_INTERVALS';
	intervals: Interval[];
}

export interface UndoAction extends Action {
	type: 'WORKOUT_CREATOR_UNDO';
}

export interface RedoAction extends Action {
	type: 'WORKOUT_CREATOR_REDO';
}

export const setIntervals = (intervals: Interval[]): SetIntervalsAction => ({
	type: SET_INTERVALS,
	intervals
});

export const undo = (): UndoAction => ({
	type: UNDO
});

export const redo = (): RedoAction => ({
	type: REDO
});

export type WorkoutCreatorAction = SetIntervalsAction | UndoAction | RedoAction;
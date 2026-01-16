/**
 * Reducer and actions for showing math problems to students
 */

import produce from 'immer';
import { ActionType, createAction, getType } from 'typesafe-actions';

import { IAchievement, IUser } from '@shared/index';

// We specify the shape of the state in an interface
export interface IStudentState {
    achievements: IAchievement[];
}

// And provide a default value for initialization
const defaultState: IStudentState = {
    achievements: [],
};

// Selectors are responsible for getting values in the state
export const studentSelectors = {
    getAchievements: (state: { student: IStudentState }) =>
        state.student.achievements,
};

// And actions allow us to mutate the state
export const studentActions = {
    reset: createAction('reset'),
    setStudentInfo: createAction(
        'teacher_SET',
        resolve => (achievements: IAchievement[]) => resolve({ achievements }),
    ),
};

export type StudentActions = ActionType<typeof studentActions>;

// The reducer is responsible for changing the state based on actions received
export const studentReducer = produce(
    (draft: IStudentState, action: StudentActions) => {
        // We switch based on the type of action
        switch (action.type) {
            case getType(studentActions.setStudentInfo): {
                const { achievements } = action.payload;
                draft.achievements = achievements;
                break;
            }
            case getType(studentActions.reset): {
                return defaultState;
            }
        }

        // Redux requires us to return the state in case the action does not match
        return draft;
    },
    defaultState,
);
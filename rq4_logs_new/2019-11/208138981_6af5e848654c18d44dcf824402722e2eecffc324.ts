/**
 * Reducer and actions for showing math problems to students
 */

import produce from 'immer';
import { ActionType, createAction, getType } from 'typesafe-actions';

import { IUser } from '@shared/index';

// We specify the shape of the state in an interface
export interface ITeacherState {
    students: IUser[];
}

// And provide a default value for initialization
const defaultState: ITeacherState = {
    students: [],
};

// Selectors are responsible for getting values in the state
export const teacherSelectors = {
    getStudents: (state: { teacher: ITeacherState }) => state.teacher.students,
};

// And actions allow us to mutate the state
export const teacherActions = {
    setTeacherInfo: createAction(
        'teacher_SET',
        resolve => (students: IUser[]) => resolve({ students }),
    ),
};

export type TeacherAction = ActionType<typeof teacherActions>;

// The reducer is responsible for changing the state based on actions received
export const teacherReducer = produce(
    (draft: ITeacherState, action: TeacherAction) => {
        // We switch based on the type of action
        switch (action.type) {
            case getType(teacherActions.setTeacherInfo): {
                const { students } = action.payload;
                draft.students = students;
                break;
            }
        }

        // Redux requires us to return the state in case the action does not match
        return draft;
    },
    defaultState,
);
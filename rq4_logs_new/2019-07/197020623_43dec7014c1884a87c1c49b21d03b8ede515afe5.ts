import { createReducer, on } from '@ngrx/store';

import { GroupActions, GroupActionsType } from './group.actions';
import { GroupModel } from './group.model';

export type GroupEntityType = { [_id: string]: GroupModel }

export interface State {
    entities: GroupEntityType
}

export const initialState: State = {
    entities: {}
}

const groupReducer = createReducer(
    initialState,
    on(GroupActions.AddGroup, (state, action) => {
        return { ...state, entities: { ...state.entities, [action.group.position]: action.group } }
    })
);

export function reducer(state: State | undefined, action: GroupActionsType) {
    return groupReducer(state, action);
}

export const getGroups = (state: State) => state.entities;
import { createStore, Reducer, combineReducers } from 'redux';
import * as Immutable from 'immutable'
import { List, Map, Set, Record } from 'immutable';

interface Debug {
    isLoading: boolean,
    error?: any
}

interface Counter {
    counter: number
}

export enum CounterActionTypes {
    DECREMENT,
    INCREMENT
}

export interface CounterAction {
    type: CounterActionTypes,
    increment: number
}

type Data = Debug & Counter;

type DataRecord = Record.IRecord<Data>;

const initialStore = Record<Data>({
    isLoading: false,
    error: null,
    counter: 0
});

const reducer = function(state: DataRecord, action: CounterAction): DataRecord {
    if (action.type === CounterActionTypes.INCREMENT) {
        return state.set("counter", state.counter + action.increment);
    } else if (action.type === CounterActionTypes.DECREMENT) {
        return state.set("counter", state.counter - action.increment);
    } else {
        return state;
    }
}

export const store = createStore(reducer);
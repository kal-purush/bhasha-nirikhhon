import {Action} from "../actions/Action";
import {Reducer} from 'redux';

let reducers:ReducerRegistration[] = [];

export function addReducer<S,A>(stateName:string,
                                actiontype:{ new(...args:any[]): A },
                                reducer:(state:S, action:A) => S,
                                initialState:S) {
    reducers = reducers.concat([
        new ReducerRegistration(stateName, actiontype, reducer, initialState)
    ]);
}

export function combineReducers(state:any, action:any) {
    let newState:{[name:string]:any};
    newState = {};

    if (state === undefined) {
        state = {};
    }

    reducers.forEach((reducerRegistration:ReducerRegistration) => {
        let partialState = state[reducerRegistration.stateName];

        if(partialState == undefined){
            partialState = reducerRegistration.initialState;
        }

        if (action instanceof reducerRegistration.actiontype) {
            partialState = reducerRegistration.reducer(partialState, action);
        }
        newState[reducerRegistration.stateName] = partialState;
    });

    return newState;
}

class ReducerRegistration {
    constructor(public stateName: string,
                public actiontype:{ new(...args:any[]): Action },
                public reducer:(state:any, action:Action) => any,
                public initialState:any) {

    }


}
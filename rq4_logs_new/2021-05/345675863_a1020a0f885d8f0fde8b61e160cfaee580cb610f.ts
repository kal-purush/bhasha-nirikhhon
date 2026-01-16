
interface ReducerAction {
    type: string,
    payload: string|number
}

export interface AddPopoverReducerState {
    name: string,
    description: string,
    priority: number,
    state: string
}

const initialState: AddPopoverReducerState = {
    name: "",
    description: "",
    priority: 2,
    state: 'not started'
}

const reducer = (prevState: any, action: ReducerAction) => {
    switch(action.type) {
        case 'name': return {...prevState, name: action.payload};
        case 'description': return {...prevState, description: action.payload};
        case 'priority': return {...prevState, priority: action.payload as number}
    }
}

export {initialState, reducer};
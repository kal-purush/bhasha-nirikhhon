import * as type from '../constant/index';

const initialState = {
    by: 'name',
    value: -1
};

var myReducers = (state = initialState, action) => {
    switch( action.type ) {
        case type.SORT:
            console.log(action)
            return action.sort
        default: 
            return state
    }
}

export default myReducers;
import { ACTIONS } from "../actions/types";

const INITIAL_STATE = null;

const templatesReducer = (state = INITIAL_STATE, action) => {
    switch (action.type) {
        case ACTIONS.LOAD_TEMPLATE:
            return { ...action.payload }
        default:
            return state;
    }
}

export default templatesReducer;
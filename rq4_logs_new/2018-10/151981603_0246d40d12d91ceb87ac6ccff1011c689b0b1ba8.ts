import { Reducer } from 'redux';
import { mainActionTypes } from '@app/constants';
import { IAction } from '@app/actions';

export interface IState {
    isAuth: any
}

const initialState: IState = {
    isAuth: localStorage.getItem('isAuth') || false
}

const reducer: Reducer<IState, IAction> = (state = initialState, action) => {
    switch (action.type) {
        case mainActionTypes.FETCH_AUTH: {
            return { ...state, isAuth: state.isAuth }
        }
        default: {
            return state
        }
    }
}

export default reducer;
import {routerStateReducer} from 'redux-router';
import {combineReducers} from 'redux';
import {ActionType, IAction} from '../actions/actions';
import {IPayloadAuthorize} from '../actions/authorize';

export interface IAppState {
	authorized?: boolean;
	login?: string;
	password?: string;
}

export interface IState {
	app: IAppState;
	router?: any;
}

function app(state:IAppState, action:IAction<any>) {
	switch (action.type) {
		case ActionType.AUTHORIZE:
			const payload = action.payload as IPayloadAuthorize;
			return Object.assign({}, state, {
				login: payload.login,
				password: payload.password,
				authorized: true
			});
		default:
			return state || {};
	}
}

export const root = combineReducers({
	app: app,
	router: routerStateReducer
});
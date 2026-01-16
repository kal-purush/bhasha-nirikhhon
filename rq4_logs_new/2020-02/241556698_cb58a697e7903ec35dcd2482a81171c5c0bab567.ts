import { AxiosError } from 'axios';
import { action as actions, ActionType, createReducer } from 'typesafe-actions';

export enum FakeActionType {
	GET_FAKE_LIST_REQUEST = 'GET_FAKE_LIST_REQUEST',
	GET_FAKE_LIST_SUCCESS = 'GET_FAKE_LIST_SUCCESS',
	GET_FAKE_LIST_ERROR = 'GET_FAKE_LIST_ERROR',
	GET_FAKE_REQUEST = 'GET_FAKE_REQUEST',
	GET_FAKE_SUCCESS = 'GET_FAKE_SUCCESS',
	GET_FAKE_ERROR = 'GET_FAKE_ERROR'
}

export const getFakeListRequest = () => actions(FakeActionType.GET_FAKE_LIST_REQUEST);
export const getFakeListSuccess = (list: any) => actions(FakeActionType.GET_FAKE_LIST_SUCCESS, { list });
export const getFakeListFailure = (error: AxiosError) => actions(FakeActionType.GET_FAKE_LIST_ERROR, { error });
export const getFakeRequest = (id: string, params: any) => actions(FakeActionType.GET_FAKE_REQUEST, { id, params });
export const getFakeSuccess = (item: any) => actions(FakeActionType.GET_FAKE_SUCCESS, { item });
export const getFakeFailure = (error: AxiosError) => actions(FakeActionType.GET_FAKE_ERROR, { error });

export const actionCreator = {
	getFakeListRequest,
	getFakeListSuccess,
	getFakeListFailure,
	getFakeRequest,
	getFakeSuccess,
	getFakeFailure
};

export interface FakeState {
	list: any[];
	item: any;
	isLoading: boolean;
	error: AxiosError | null;
	id: string;
	params: any;
}

export type FakeAction = ActionType<typeof actionCreator>;

export const initialState: FakeState = {
	list: [],
	item: {},
	id: '',
	isLoading: false,
	error: null,
	params: undefined
};

export default createReducer<FakeState, FakeAction>(initialState, {
	[FakeActionType.GET_FAKE_LIST_REQUEST]: state => {
		return {
			...state,
			isLoading: true,
			error: null
		};
	},
	[FakeActionType.GET_FAKE_LIST_SUCCESS]: (state, action) => {
		return { ...state, isLoading: false, error: null, list: action.payload.list };
	},
	[FakeActionType.GET_FAKE_LIST_ERROR]: (state, action) => {
		return { ...state, isLoading: false, error: action.payload.error };
	},
	[FakeActionType.GET_FAKE_REQUEST]: (state, action) => {
		return { ...state, isLoading: true, error: null, params: action.payload.params, id: action.payload.id };
	},
	[FakeActionType.GET_FAKE_SUCCESS]: (state, action) => {
		return { ...state, isLoading: false, error: null, item: action.payload.item };
	},
	[FakeActionType.GET_FAKE_ERROR]: (state, action) => {
		return { ...state, isLoading: false, error: action.payload.error };
	}
});
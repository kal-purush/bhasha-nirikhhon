import { Action } from 'redux';

export const SET_CURRENT_PAGE = 'SET_CURRENT_PAGE';
export const DUMMY_ACTION = 'DUMMY_ACTION';

export type Page = 'data' | 'workout-creator';

export interface SetCurrentPageAction extends Action {
	type: 'SET_CURRENT_PAGE';
	page: Page;
}

interface DummyAction extends Action {
	type: 'DUMMY_ACTION';
}

export const setCurrentPage = (page: Page): SetCurrentPageAction => ({
	type: SET_CURRENT_PAGE,
	page
});

export type ViewAction = SetCurrentPageAction | DummyAction;
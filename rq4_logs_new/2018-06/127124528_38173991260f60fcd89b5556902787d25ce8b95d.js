import reducer, {
	START,
	initialState,
	RETRIEVE_LIST_SUCCESS,
	RETRIEVE_ONE_SUCCESS, UPDATE_SUCCESS, CREATE_SUCCESS, DELETE_SUCCESS, ERROR,
} from "./index";

const admins = [{
	id: 1,
	status: 0,
	first_name: 'Jack',
	last_name: 'Daniels',
	email: 'jack@gmail.com',
}, {
	id: 2,
	status: 2,
	first_name: 'Ronald',
	last_name: 'Reed',
	email: 'ronald@admin.com',
}];
const admin = admins[1];
const newAdmin = {
	id: 3,
	status: 0,
	first_name: 'John',
	last_name: 'Dinn',
	email: 'john@gmail.com',
};

describe('admins reducer', () => {
	it('start', () => {
		const nextState = reducer(initialState, { type: START });
		expect(nextState.get('isLoading')).toBeTruthy();
	});
	it('retrueve list', () => {
		const currentState = initialState.set('isLoading', true);
		const nextState = reducer(currentState, { type: RETRIEVE_LIST_SUCCESS, admins });
		expect(nextState.get('isLoading')).toBeFalsy();
		expect(nextState.get('list').toJS()).toEqual({ 1: admins[0], 2: admins[1] });
		expect(nextState.get('pageNumber')).toEqual(2);
		expect(nextState.get('hasMore')).toBeFalsy();
	});
	it('retrieve one', () => {
		const currentState = initialState.set('isLoading', true);
		const nextState = reducer(currentState, { type: RETRIEVE_ONE_SUCCESS, admin })
		expect(nextState.get('isLoading')).toBeFalsy();
		expect(nextState.get('list').toJS()).toEqual({ 2: admin });
	});
	it('update', () => {
		const currentState = initialState.merge({
			isLoading: true,
			list: { 1: admins[0], 2: admins[1] }
		});
		const nextState = reducer(currentState, {
			type: UPDATE_SUCCESS,
			admin: { first_name: 'John' },
			adminId: 1
		});
		expect(nextState.get('isLoading')).toBeFalsy();
		expect(nextState.get('list').size).toBe(2);
		expect(nextState.getIn(['list', '1']).toJS()).toEqual({
			id: 1,
			status: 0,
			first_name: 'John',
			last_name: 'Daniels',
			email: 'jack@gmail.com',
		});
	});
	it('create', () => {
		const currentState = initialState.merge({
			isLoading: true,
			list: { 1: admins[0], 2: admins[1] },
		});
		const nextState = reducer(currentState, {
			type: CREATE_SUCCESS,
			admin: newAdmin,
			adminId: 3
		});
		expect(nextState.get('isLoading')).toBeFalsy();
		expect(nextState.get('list').size).toBe(3);
		expect(nextState.getIn(['list', '3']).toJS()).toEqual({
			id: 3,
			email: 'john@gmail.com',
			first_name: 'John',
			last_name: 'Dinn',
			status: 0
		});
	});
	it('delete', () => {
		const currentState = initialState.merge({
			isLoading: true,
			list: { 1: admins[0], 2: admins[1] },
		});
		const nextState = reducer(currentState, {
			type: DELETE_SUCCESS,
			adminId: 1,
		});
		expect(nextState.get('isLoading')).toBeFalsy();
		expect(nextState.get('list').toJS()).toEqual({ 2: admins[1] });
	});
	it('error', () => {
		const currentState = initialState.set('isLoading', true);
		const nextState = reducer(currentState, { type: ERROR });
		expect(nextState.get('isLoading')).toBeFalsy();
	})
});
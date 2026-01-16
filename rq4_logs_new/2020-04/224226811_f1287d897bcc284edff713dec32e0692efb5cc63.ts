import { Action } from 'redux';
import { rootReducer } from '@state/reducers';
import * as Actions from '@state/actions/workoutCreatorActions';

const getInitialState = () => rootReducer(undefined, {} as Action);

describe('Workout Creator Store', () => {
	it('initial state should match snapshot', () => {
		const initialState = getInitialState();
		expect(initialState.workoutCreator).toMatchSnapshot();
	});

	it('updates intervals', () => {
		const store = getInitialState();

		const intervalsToSet = [{ color: '', length: 60, intensity: 1.2 }];
		const updatedStore = rootReducer(store, Actions.setIntervals(intervalsToSet));

		expect(updatedStore.workoutCreator.currentIntervals).toEqual(intervalsToSet);
		expect(updatedStore.workoutCreator.history.length).toEqual(2);
		expect(updatedStore.workoutCreator.currentHistoryPosition).toEqual(1);
	});

	it('undo redo', () => {
		const initialStore = getInitialState();

		const afterInitialUpdate = rootReducer(initialStore, Actions.setIntervals([]));
		expect(afterInitialUpdate.workoutCreator.currentIntervals).toEqual([]);

		const afterUndo = rootReducer(afterInitialUpdate, Actions.undo());
		expect(afterUndo.workoutCreator.currentIntervals).toEqual(
			initialStore.workoutCreator.currentIntervals
		);

		const afterRedo = rootReducer(afterUndo, Actions.redo());
		expect(afterRedo.workoutCreator.currentIntervals).toEqual(
			afterInitialUpdate.workoutCreator.currentIntervals
		);
	});
});
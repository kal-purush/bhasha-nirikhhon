import { CourseItemsLoadedAction } from './course-items-loaded.action';
import { AppState } from '../model/app-state';
import { oneItem } from '../model/items.service.spec';


describe('CourseItemsLoadedAction', () => {

  it('should create a nice state after the course items have been loaded', () => {

    let action = new CourseItemsLoadedAction(oneItem);

    let initState = AppState.createEmptyState();

    let resultState = action.createNewState(initState);
    let uiState = resultState.uiState;

    expect(uiState.isLoading).toBe(false, 'there is no http call in progress');
    expect(resultState.items.length).toBe(1, 'there should be one item in the appState');

  });

});
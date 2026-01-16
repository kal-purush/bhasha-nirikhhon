import {ErrorBackendCallAction} from './error-backend-call.action';
import {AppState} from '../model/app-state';


describe('ErrorBackendCallAction', () => {

  it('should create a nice state after an error occured during a backend call', () => {

    let action = new ErrorBackendCallAction();

    let initState = AppState.createEmptyState();

    let resultState = action.createNewState(initState);
    let uiState = resultState.uiState;

    expect(uiState.isLoading).toBe(false, 'there is no http call in progress');
    expect(uiState.isDataSend).toBe(false, 'there is no data send to the server - because there is an error');
    expect(uiState.isHttpError).toBe(true, 'yes there is an error');

  });

});
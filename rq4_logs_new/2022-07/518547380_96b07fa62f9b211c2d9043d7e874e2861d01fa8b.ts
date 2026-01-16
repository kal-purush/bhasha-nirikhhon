import { handleOpenClose, handleMessage } from '../actions';
import alertReducer from '../reducer';
import { State } from '../types';

describe('Alert handler reducer', () => {
  const initialState: State = {
    id: 0,
    open: false,
    message: '',
  };
  it('should handle initial state', () => {
    expect(alertReducer(undefined, { type: 'unknown' })).toEqual({
      id: 0,
      open: false,
      message: '',
    });
  });
  it('should handle alert true', () => {
    expect(alertReducer(initialState, handleOpenClose(true))).toEqual({
      id: 0,
      open: true,
      message: '',
    });
  });
  it('should handle alert message change', () => {
    expect(
      alertReducer({ ...initialState, open: true }, handleMessage('All inputs are empty.')),
    ).toEqual({
      id: 0,
      open: true,
      message: 'All inputs are empty.',
    });
  });
  it('should handle alert message clear', () => {
    expect(alertReducer({ ...initialState, open: true }, handleMessage(''))).toEqual({
      id: 0,
      open: true,
      message: '',
    });
  });
  it('should handle alert false', () => {
    expect(alertReducer({ ...initialState, open: true }, handleOpenClose(false))).toEqual({
      id: 0,
      open: false,
      message: '',
    });
  });
});
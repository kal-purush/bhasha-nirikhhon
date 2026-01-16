import {
  RECOVERY_REQUEST,
  RECOVERY_SUCCESS,
  RECOVERY_FAILED,

} from '../actions/recoveryActions.js';

const initialState = {
  loading: false,
  loaded: false,
  err: null,
  message: null,
  email: null,
  bookName: '',
  category: 0,
};

export default (state = initialState, action) => {
  switch (action.type) {
    case RECOVERY_REQUEST:
      return {
        ...state,
        loading: true,
        loaded: false,
      };
    case RECOVERY_SUCCESS:
      return {
        ...state,
        loading: false,
        loaded: true,
      };
    case RECOVERY_FAILED:
      return {
        ...state,
        loading: false,
        loaded: false,
        err: action.payload.err,
      };
    default:
      return initialState;
  }
};
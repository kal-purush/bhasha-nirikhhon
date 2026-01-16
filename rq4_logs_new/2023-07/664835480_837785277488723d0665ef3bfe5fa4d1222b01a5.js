import {
  TOGGLE_SMS_NOTIFICATION,
  TOGGLE_PUSH_NOTIFICATION,
  TOGGLE_EMAIL_NOTIFICATION,
} from './actions';

const initialState = {
  smsNotification: false,
  pushNotification: false,
  emailNotification: false,
};

export default function settingsReducer(state = initialState, action) {
  switch (action.type) {
    case TOGGLE_SMS_NOTIFICATION:
      return {
        ...state,
        smsNotification: action.enabled,
      };
    case TOGGLE_PUSH_NOTIFICATION:
      return {
        ...state,
        pushNotification: action.enabled,
      };
    case TOGGLE_EMAIL_NOTIFICATION:
      return {
        ...state,
        emailNotification: action.enabled,
      };
    default:
      return state;
  }
}
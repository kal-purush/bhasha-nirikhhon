import { initialLogInState } from "./initalStateLogin";
import { logInType, logOutType } from "./actionTypesLogin";

const logInReducer = (
  state = initialLogInState,
  action: { type: string; payload: string }
): { loggedIn: boolean; userName: string } => {
  switch (action.type) {
    case logInType:
      return {
        ...state,
        loggedIn: true,
        userName: action.payload,
      };
    case logOutType:
      return {
        ...state,
        loggedIn: false,
        userName: initialLogInState.userName,
      };
    default:
      return state;
  }
};

export default logInReducer;
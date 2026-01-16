import { SET_USER } from "./types"

export const registerUser = (userData) => {
  return {
    type: SET_USER, // set_user is a type? Why does it need type?
    payload: userData // information of the dispatch call.
  }

  // AS  soon as this call is dispatched above... the reducer in the authReducer will listen to this and trigger and write this information above into the redux Store. It's being triggered in the Register component.
}
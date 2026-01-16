import { Dispatch } from "redux";
// import { AppStoreType } from "./store-test";

const initialState = {
   value: 1000
}

type InitialStateType = typeof initialState;

const counterReducer = (state: InitialStateType = initialState, action: ActionTypes): InitialStateType => {
   switch (action.type) {
      case 'INC-COUNTER-VALUE': {
         return { ...state, value: state.value + 1 }
      }
      case 'SET-VALUE-FROM-LOCAL-STORAGE': {
         return {
            ...state, value: action.value
         }
      }
      default:
         return state
   }
}

const incCounterValueAC = () => ({ type: 'INC-COUNTER-VALUE' } as const)
const setValueFromLocalStorageAC = (value: number) => ({ type: 'SET-VALUE-FROM-LOCAL-STORAGE', value } as const)
type IncCounterValueActionType = ReturnType<typeof incCounterValueAC>
type SetValueFromLocalStorageActionType = ReturnType<typeof setValueFromLocalStorageAC>

// THUNK

// export const incCounterValueTC = () => (dispatch: Dispatch, getState: () => AppStoreType) => {
// let currentValue = getState().counter.value;
// localStorage.setItem('counterValue', JSON.stringify(currentValue + 1))
//    dispatch(incCounterValueAC())
// }

// export const setValueFromLocalStorageTC = () => (dispatch: Dispatch) => {
//    let valueAsString = localStorage.getItem('counterValue');
//    if (valueAsString) {
//       let newValue = JSON.parse(valueAsString)
//       dispatch(setValueFromLocalStorageAC(newValue))
//    }
// }

type ActionTypes = IncCounterValueActionType | SetValueFromLocalStorageActionType;
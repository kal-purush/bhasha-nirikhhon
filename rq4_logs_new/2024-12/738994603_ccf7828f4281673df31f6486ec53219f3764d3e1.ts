import {
  type AlgorithmAction,
  AlgorithmActionType,
  type AlgorithmState,
} from "./types";

export const algorithmReducer = (
  state: AlgorithmState,
  action: AlgorithmAction
): AlgorithmState => {
  switch (action.type) {
    case AlgorithmActionType.SET_SELECTED_ALGORITHM:
      return {
        ...state,
        selected: action.payload,
      };
    case AlgorithmActionType.SET_IS_ALGORITHM_RUNNING:
      return {
        ...state,
        isRunning: action.payload,
      };
    case AlgorithmActionType.SET_IS_ALGORITHM_SHOWING_RESULT:
      return {
        ...state,
        isShowingResult: action.payload,
      };

    default:
      return state;
  }
};
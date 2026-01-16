/* eslint-disable indent */

enum PageTypes {
  ADD_PAGE = 'ADD_PAGE',
  RESET_PAGE = 'RESET_PAGE',
}

interface PageState {
  page: number;
}

const initialState = {
  page: 0,
};

interface ActionType {
  type: string;
  payload?: number;
}

export const pageReducer = (state = initialState, action: ActionType): PageState => {
  switch (action.type) {
    case PageTypes.ADD_PAGE:
      return action.payload > 0 ? { ...state, page: state.page + action.payload } : { ...state, page: state.page + 30 };
    case PageTypes.RESET_PAGE:
      return { ...state, page: 0 };
    default: {
      return state;
    }
  }
};
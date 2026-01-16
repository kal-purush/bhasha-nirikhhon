import { Action } from 'redux';

export interface Keep {
  id: string;
  value: string;
  isOpen: boolean;
}

interface KeepAddAction extends Action {
  type: '@@keep/add';
  payload: {
    keep: Keep | Keep[];
  };
}

interface KeepDeleteAction extends Action {
  type: '@@keep/delete';
  payload: {
    id: string;
  };
}

interface KeepUpdateAction extends Action {
  type: '@@keep/update';
  payload: {
    id: string;
    value: string;
  };
}

interface KeepOpenAction extends Action {
  type: '@@keep/open';
  payload: {
    id: string;
  };
}

interface KeepCloseAction extends Action {
  type: '@@keep/close';
  payload: {
    id: string;
  };
}

export type KeepActionTypes = KeepAddAction | KeepDeleteAction | KeepUpdateAction | KeepOpenAction | KeepCloseAction;

export const keepActions = {
  add: (keep: Keep | Keep[]): KeepActionTypes => ({
    type: '@@keep/add',
    payload: {
      keep
    }
  }),
  delete: (id: string): KeepActionTypes => ({
    type: '@@keep/delete',
    payload: {
      id
    }
  }),
  update: (id: string, value: string): KeepActionTypes => ({
    type: '@@keep/update',
    payload: {
      id,
      value
    }
  }),
  open: (id: string): KeepActionTypes => ({
    type: '@@keep/open',
    payload: {
      id
    }
  }),
  close: (id: string): KeepActionTypes => ({
    type: '@@keep/close',
    payload: {
      id
    }
  })
};

export interface KeepsState {
  items: Keep[];
}

export const keepsReducer = (state: KeepsState = { items: [] }, action: KeepActionTypes): KeepsState => {
  switch (action.type) {
    case '@@keep/add':
      if (Array.isArray(action.payload.keep)) {
        return {
          ...state,
          items: [...state.items, ...action.payload.keep]
        };
      } else {
        return {
          ...state,
          items: [...state.items, action.payload.keep]
        };
      }
    case '@@keep/delete':
      return {
        ...state,
        items: state.items.filter(item => item.id !== action.payload.id)
      };
    case '@@keep/update':
      return {
        ...state,
        items: state.items.map(item => {
          if (item.id === action.payload.id) {
            item.value = action.payload.value;
          }
          return item;
        })
      };
    case '@@keep/open':
      return {
        ...state,
        items: state.items.map(item => {
          if (item.id === action.payload.id) {
            item.isOpen = true;
          }
          return item;
        })
      };
    case '@@keep/close':
      return {
        ...state,
        items: state.items.map(item => {
          if (item.id === action.payload.id) {
            item.isOpen = false;
          }
          return item;
        })
      };
    default:
      return state;
  }
};
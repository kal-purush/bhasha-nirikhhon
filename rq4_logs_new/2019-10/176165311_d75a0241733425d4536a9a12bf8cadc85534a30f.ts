import { createStore, Dispatch } from 'redux';

export const defaultState = {
  items: Array(10)
    .fill(0)
    .map((_, i) => ({
      id: String(i),
      updates: 0,
    })),
};

export const store = createStore<typeof defaultState, { type: string; payload?: any }, any, any>(
  (state = defaultState, action) => {
    switch (action.type) {
      case 'MODIFY': {
        const index = action.payload;
        const newItem = {
          ...state.items[index],
          updates: state.items[index].updates + 1,
        };

        const items = [...state.items];
        items[index] = newItem;

        return { ...state, items };
      }
      default:
        return state;
    }
  }
);

export const mapAllItemsToProps = ({ items }: typeof defaultState) => ({ items });
export const mapSingleItemToProps = ({ items }: typeof defaultState, { index }: { index: number }) => ({
  item: items[index],
});

export const mapDispatchToProps = (dispatch: Dispatch) => ({
  updateItem: (index: number) => dispatch({ type: 'MODIFY', payload: index }),
});
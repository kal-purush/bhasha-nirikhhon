import { combineReducers } from 'redux';
import contactsTypes from './contacts-types';

const itemsReducer = (state = [], { type, payload }) => {
  switch (type) {
    case contactsTypes.ADD_ITEM:
      return state.some(item => item.name === payload[0].name)
        ? // eslint-disable-next-line
          alert(
            `${payload[0].name
              .split(' ')
              .map(string => string.charAt(0).toUpperCase() + string.slice(1))
              .join(
                ' ',
              )} is already in contacts. Change contact's name or delete old.`,
          )
        : [...state, ...payload];
    case contactsTypes.DELETE_ITEM:
      return state.filter(({ id }) => id !== payload);
    default:
      return state;
  }
};
const filterReducer = (state = '', { type, payload }) => {
  switch (type) {
    case contactsTypes.CHANGE_FILTER:
      return payload;

    default:
      return state;
  }
};

const contactsReducer = combineReducers({
  items: itemsReducer,
  filter: filterReducer,
});

export default contactsReducer;
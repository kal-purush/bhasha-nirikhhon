const defaultState = 0;

const INCR = 'INCR';
const DECR = 'DECR';
const INPUT = 'INPUT';
const RESET = 'RESET';

export const countReducer = (state = defaultState, action) => {
  switch (action.type) {
    case INCR:
      return state + action.payload
    case DECR:
      return state - action.payload
    case INPUT:
      return action.payload
    case RESET:
      return 0
    default:
      return state
  }
};

// Генератор экшена
// Функция, которая возвращает объект action
export const incrAction = (payload) => ({ type: INCR, payload });
export const decrAction = (payload) => ({ type: DECR, payload });
export const inputAction = (payload) => ({ type: INCR, payload });
export const resetAction = () => ({ type: RESET });
import { getUID } from '../../utils/'
import { Action, ADD_TODO, TOGGLE_TODO } from './actions'

export interface ITodo {
  text: string
  id: string
  complete: boolean
}

export const todos = (state: ITodo[] = [], action: Action) => {
  switch (action.type) {
    case ADD_TODO:
      return [
        ...state,
        {
          complete: false,
          id: getUID(),
          text: action.text
        }
      ]
    case TOGGLE_TODO:
      return state.map((todo, i) => {
        if (i === action.index) {
          todo.complete = !todo.complete
        }
        return todo
      })
    default:
      return state
  }
}
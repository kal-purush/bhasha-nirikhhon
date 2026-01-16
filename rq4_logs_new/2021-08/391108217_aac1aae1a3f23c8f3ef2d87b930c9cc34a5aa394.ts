// action & reducer
import { Action } from '../constant'
import { EditorState } from 'draft-js'

export interface EditorItem {
  title: string
  content: EditorState
  canShow: boolean
  id: number | undefined
}

export const defaultState: EditorItem = {
  title: '',
  content: EditorState.createEmpty(),
  canShow: false,
  id: undefined
}

export const actionType: { [p: string]: string } = {
  initEditor: 'INIT_EDITOR',
  setEditor: 'SET_EDITOR',
  setTitle: 'SET_TITLE',
  setCanShow: 'SET_CAN_SHOW',
  setId: 'SET_ID'
}

export default (state = defaultState, action: Action<any>): EditorItem => {
  switch (action.type) {
    case actionType.initEditor:
      return Object.assign({}, state, {
        content: EditorState.createEmpty()
      })
    case actionType.setEditor:
      return Object.assign({}, state, {
        content: action.payload
      })
    case actionType.setTitle:
      return Object.assign({}, state, {
        title: action.payload
      })
    case actionType.setCanShow:
      return Object.assign({}, state, {
        canShow: !state.canShow
      })
    case actionType.setId:
      return Object.assign({}, state, {
        id: action.payload
      })
    default:
      return state
  }
}
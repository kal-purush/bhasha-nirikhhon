import { createContext } from 'react'

type Msgtype = {
  id: number
  msg: string
}

type MsgListType = {
  msgList: Msgtype[]
  nextMsgId: number
}

type AddMsgType = {
  type: 'ADD_MSG'
  payload: { id: number; msg: string }
}

type MsgActionType = AddMsgType

export const initialMsg = {
  msgList: [],
  nextMsgId: 0,
}

export const MsgContext = createContext(
  {} as {
    state: MsgListType
    dispatch: React.Dispatch<MsgActionType>
  }
)

export const msgReducer = (
  state: MsgListType,
  action: MsgActionType
): MsgListType => {
  switch (action.type) {
    case 'ADD_MSG':
      return {
        ...state,
        msgList: [
          ...state.msgList,
          { id: action.payload.id, msg: action.payload.msg },
        ],
        nextMsgId: state.nextMsgId + 1,
      }
    default:
      return state
  }
}
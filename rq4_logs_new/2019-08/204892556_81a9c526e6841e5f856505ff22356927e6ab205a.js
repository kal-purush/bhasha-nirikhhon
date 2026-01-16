import { PLAY_TYPES } from './action-type'
// import Axios from 'axios'

export const actionClickNumber = (value) => ({
  type: PLAY_TYPES.CLICK_NUMBER,
  value
})

export const actionClickGuess = () => ({
  type: PLAY_TYPES.CLICK_GUESS
})

export const actionDeleteNumber = () => ({
  type: PLAY_TYPES.DELETE_NUMBER
})

export const actionResetList = () => ({
  type: PLAY_TYPES.RESET_GAME
})

export const actionCloseGameover = () => ({
  type: PLAY_TYPES.CLOSE_GAMEOVER
})

export const actionAddUseTime = () => ({
  type: PLAY_TYPES.ADD_USE_TIME
})

// export const getTodoList = () => {
//   return (dispatch) => {
//     Axios.get('https://www.easy-mock.com/mock/5d5b82903e3a6541823d7990/react-test/getList').then((res) => {
//       const data = res.data
//       const action = getListAction(data)
//       dispatch(action)
//     })
//   }
// }
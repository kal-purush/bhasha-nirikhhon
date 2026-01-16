import { FETCH_TEAM } from '../constants/team'
import  notification  from 'antd/lib/notification';

const fetch_team = (data: any) => ({
  type: FETCH_TEAM,
  data: data
})

// const add_message = (data: any) => ({
//   type: ADD_MESSAGE,
//   data: data
// })

export function fetchTeam (user: any) {
  return (dispatch: any) => {
    dispatch(fetch_team(user))
  }
}

export function errorTeam (msg: string) {
  notification.error({
    message:' 获取团队列表失败!',
    description: msg,
    duration: 2
  })
}
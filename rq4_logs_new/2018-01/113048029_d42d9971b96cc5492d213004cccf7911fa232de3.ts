import { FETCHBASE_MODEL, FETCHCUSTOM_MODEL } from '../constants/model'
import  notification  from 'antd/lib/notification';

const fetchbase_model = () => ({
  type: FETCHBASE_MODEL,
})

const fetchcustom_model = (data: any) => ({
  type: FETCHCUSTOM_MODEL,
  data: data
})


// const add_message = (data: any) => ({
//   type: ADD_MESSAGE,
//   data: data
// })

export function fetchBaseModel () {
  return (dispatch: any) => {
    dispatch(fetchbase_model())
  }
}

export function fetchCustomModel (id: any) {
  return (dispatch: any) => {
    dispatch(fetchcustom_model(id))
  }
}


export function errorModel(msg: string) {
  notification.error({
    message:' 获取模型失败!',
    description: msg,
    duration: 2
  })
}
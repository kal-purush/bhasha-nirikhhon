import Axios from 'axios';

type SuccessType = {
    success:boolean
}

type ResponseType = {
    errorText: string
    info: string
    yourBody: SuccessType
    success: boolean
    yourQuery: {}
}

export function authAPI(boolean:boolean){
    return Axios.post<ResponseType>('https://neko-cafe-back.herokuapp.com/auth/test',{success: boolean})
}

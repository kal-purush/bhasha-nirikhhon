import { userResource } from './resource'

export const userLogin = (data) => userResource.save({ event: 'login.do' }, data)
export const userRegister = (data) => userResource.save({ event: 'register.do' }, data)
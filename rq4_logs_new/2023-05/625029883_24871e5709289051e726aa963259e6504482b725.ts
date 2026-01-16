import { IUser } from '../../../models/IUser';
import { AuthActionEnum, SetUserAction } from './types';


export const AuthActionCreators = {
    setUser: (payload: IUser): SetUserAction => ({
        type: AuthActionEnum.SET_USER,
        payload: payload
    })
        setError: (payload: IUser): SetUserAction => ({
        type: AuthActionEnum.SET_USER,
        payload: payload
    })
        setLoading: (payload: IUser): SetUserAction => ({
        type: AuthActionEnum.SET_USER,
        payload: payload
    })
        setAuth: (payload: boolean): SetUserAction => ({
        type: AuthActionEnum.SET_USER,
        payload: payload
    })
};
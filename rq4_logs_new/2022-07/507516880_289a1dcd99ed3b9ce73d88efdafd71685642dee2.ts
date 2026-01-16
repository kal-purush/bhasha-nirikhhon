import {UserAction} from "../action-types/user";

export  const setUserName = (userName: string) => ({
    type: UserAction.SET_USER_NAME,
    payload: userName,
})

export  const setUserToken = (userToken: string) => ({
    type: UserAction.SET_USER_TOKEN,
    payload: userToken,
})

export  const setUserId = (userId: string) => ({
    type: UserAction.SET_USER_ID,
    payload: userId,
})
export  const setIsLoggedIn = (isLoggedIn: boolean) => ({
    type: UserAction.SET_IS_LOGGED,
    payload: isLoggedIn,
})

export  const setErrorMsg = (errorMsg: string) => ({
    type: UserAction.SET_ERROR_MSG,
    payload: errorMsg,
})
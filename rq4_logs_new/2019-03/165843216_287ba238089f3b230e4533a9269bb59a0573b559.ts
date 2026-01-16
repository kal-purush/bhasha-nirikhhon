import userService from "../services/userService";

const userReducer = (state = null, action: any) => {
  switch (action.type) {
    case 'LOGIN':
      return (state = action.data)
    case 'LOGOUT':
      return (state = null)
    default:
      return state
  }
}

export const actionForLogin = () => {
  return async (dispatch: any) => {
    const user = await userService.login()
    userService.setCredentials(user.id)
    await window.localStorage.setItem('loggedMystashappUser', JSON.stringify(user))
    dispatch({
      type: 'LOGIN',
      data: user
    })
    // console.log(user)
    return user
  }
}
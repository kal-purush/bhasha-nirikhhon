import { authenticateUserRequest } from '../api/userApi';
import { AxiosError } from "axios";
import { ErrorData, User } from '../api/models';
import { UserReducerActionTypes } from '../actions/actionTypes';
import { UserInfo } from '../reducers/dto/userReducerDto';
import { unknownUser } from '../api/models/User';
import { removeCurrentUserFromLocalStorage, storeCurrentUserFromLocalStorage } from '../utils/localStorageUtils';
import { createAndDispachNewNotification } from './notificationActions';
import { NotificationMessageType } from '../reducers/notificationsReducer';


// export const login = (username, password) => (dispatch) => {
export const loginAction = (username: string, password: string) => (dispatcher: any) => {

  return authenticateUserRequest(username, password).then(
    (data: User) => {
      storeCurrentUserFromLocalStorage(data);
      const payload: UserInfo = {
        user: data,
        loginerror: false,
        errmessage: '',
        loggedIn: true
      }
      dispatcher({
        type: UserReducerActionTypes.LOGIN_SUCCESS,
        payload,
      });
      // Notification message
      const message = "Welcome " + data.username;
      createAndDispachNewNotification(dispatcher, NotificationMessageType.SUCCESS, message);

    })
    .catch((e) => handlerError(e, dispatcher));

}

export const logoutAction = () => (dispatcher: any) => {
  removeCurrentUserFromLocalStorage();
  const payload: UserInfo = {
    user: unknownUser,
    loginerror: false,
    errmessage: '',
    loggedIn: false
  }
  // Dispatch Logout
  dispatcher({
    type: UserReducerActionTypes.LOGOUT,
    payload,
  });
  // Notification message
  const message = "Logout requested";
  createAndDispachNewNotification(dispatcher, NotificationMessageType.INFO, message);
  
}

const handlerError = (error: AxiosError, dispatcher: any) => {
  removeCurrentUserFromLocalStorage();

  let errmessage =
    (error.response &&
      error.response.data &&
      error.response.data.message) ||
    error.message ||
    error.toString();

  if (error.response) {
    // Request made and server responded
    const errorData: ErrorData = error.response.data;
    if (errorData) {
      console.log(`${errorData.message}: ${errorData.details.join()}`);
      errmessage = `${errorData.message}: ${errorData.details.join()}`;
    }


  } else if (error.request) {
    // The request was made but no response was received
    console.log(error.request);
    errmessage = "The request was made but no response was received";
  } else {
    // Something happened in setting up the request that triggered an Error
    console.log('Error', error.message);
    errmessage = 'Error:' + error.message;
  }

  const payload: UserInfo = {
    user: unknownUser,
    loginerror: true,
    errmessage: errmessage,
    loggedIn: false
  }
  dispatcher({
    type: UserReducerActionTypes.LOGIN_ERROR,
    payload,
  });
  // Notification message
  const message = errmessage;
  createAndDispachNewNotification(dispatcher, NotificationMessageType.ERROR, message);

}
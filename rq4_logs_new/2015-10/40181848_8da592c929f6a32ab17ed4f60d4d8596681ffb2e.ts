import Parse from 'parse';
import { appDispatcher } from './../../appDispatcher';
import { AUTH_ACTION_TYPES } from './../../constants';

class LoginAction {
  execute(username: string, password: string) {
    return new Promise((resolve, reject) => {
      const newUser = Parse.User();
      newUser.set('username', username);
      newUser.set('password', password);
      newUser.set('email', username);
      
      newUser.signUp(null, {
        success: (user: any) => {
          appDispatcher.dispatch(AUTH_ACTION_TYPES.SIGN_UP_SUCCESS);
          return resolve(user);
        },
        error: (user: any, error: any) => {
          return reject(error);
        }
      });
    });
  }
}

export default {
  logoutAction: new LoginAction()
};
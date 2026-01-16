import { Parse } from 'parse';
import { EventEmitter } from 'events';
import { EVENT_TYPES, AUTH_ACTION_TYPES } from './../../constants';
import { appDispatcher } from './../../appDispatcher';

class CurrentUserStore extends EventEmitter {
  private isLoggedIn = false;
  private parseUser: any = null;

  constructor() {
    super();
    appDispatcher.register(this.onAppDispatch.bind(this));
  }

  init() {
    this.parseUser = Parse.User.current();
    this.isLoggedIn = this.parseUser && this.parseUser.authenticated();
  }

  getIsLoggedIn() {
    return this.isLoggedIn;
  }

  addLoginListener(callback) {
    this.on(EVENT_TYPES.AUTH_LOGIN, callback);
    return this;
  }
  removeLLoginListener(callback) {
    this.on(EVENT_TYPES.AUTH_LOGIN, callback);
    return this;
  }

  addLogoutListener(callback) {
    this.on(EVENT_TYPES.AUTH_LOGOUT, callback);
    return this;
  }
  removeLLogoutListener(callback) {
    this.on(EVENT_TYPES.AUTH_LOGOUT, callback);
    return this;
  }

  onAppDispatch(data) {
    console.log(data);
    switch (data.type) {
      case AUTH_ACTION_TYPES.LOG_IN_SUCCESS:
        this.isLoggedIn = true;
        this.emit(EVENT_TYPES.AUTH_LOGIN);
        break;

      case AUTH_ACTION_TYPES.LOG_OUT_SUCCESS:
        this.isLoggedIn = false;
        this.emit(EVENT_TYPES.AUTH_LOGOUT);
        break;

      default:
        break;
    }
  }
}

export default {
  currentUserStore: new CurrentUserStore()
};
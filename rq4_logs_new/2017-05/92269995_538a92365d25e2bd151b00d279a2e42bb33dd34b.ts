import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';

// Import API.
import API from '../../utils/api';

// Messages used by the AuthServiceProvider.
const messages = {
  invalidCreds: 'Invalid Credentials',
  emailPassWrong: 'Email or Password is wrong',
  errOccured: 'An error occured'
};
/**
 * Class detailing the structure of a user Object.
 */
export class User {
  name: string;
  gender: string;
  /**
   * @constructor
   * @param {string} name - name of the user.
   * @param {string} gender - gender of the user.
   */
  constructor(name: string, gender: string) {
    this.name = name;
    this.gender = gender;
  }
}

/**
 * Class Managing the Authentication of the application.
 */
@Injectable()
export class AuthServiceProvider {
  // Stores the current user information.
  currentUser: User;
  /**
   * Used to login to the application.
   * This method mocks the login. It fires a request to get one people from API.
   * Stores the response to the current user property.
   * @param {Object} credentials - Credentials used to log in.
   * @param {string} credentials.email - Email of the user.
   * @param {string} credentials.password - Password of the user.
   * @return {Promise} Promise which resolves to true if succesful.
   */
  public login(credentials) {
    if(credentials.email === null || credentials.password === null) {
      return Promise.reject(messages.invalidCreds);
    } else {
      return this.http.get(API.root + API.path.login)
        .toPromise()
        .then(response => response.json())
        .then((res) => {
          let access = (credentials.password === 'test' && credentials.email === 'test');
          if(access) {
            // Store the current user
            this.currentUser = new User(res.name, res.gender);
            return true;
          }
          else {
            return Promise.reject(messages.emailPassWrong);
          }
      }).catch(this.handleError);
    }
  }
  /**
   * Used to handle error from the login API request.
   * Logs the error in console.
   * Ideally the error should be pushed to third party log service.
   * @param {Any} error - Error object.
   * @param {string} credentials.email - Email of the user.
   * @return {Promise} A rejected Promise.
   */
  private handleError(error: any): Promise<any> {
    console.error(messages.errOccured, error);
    return Promise.reject(error.message || error);
  }
  /**
   * Gets the current user info.
   * @return {User} Object of type User.
   */
  public getUserInfo() : User {
    return this.currentUser;
  }
  /**
   * Logsout the user from the application.
   * Set the current user to null.
   * @return {Promise} Resolves to true.
   */
  public logout() {
    this.currentUser = null;
    return Promise.resolve(true);
  }
  /**
   * @constructor
   * @param {Http} http - http module from angular.
   */
  constructor(public http: Http) {}

}
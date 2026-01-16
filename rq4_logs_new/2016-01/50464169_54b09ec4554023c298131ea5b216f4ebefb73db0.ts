import {User} from '../models/user';
var FB = require('fb');

export module FacebookService {
    export function getUser(access_token: string): Promise<User.User> {
        return new Promise((resolve, reject) => {
            FB.setAccessToken(access_token);
            FB.api('/me', {fields: ['id', 'name', 'email']}, (res) => {
                if (res && res.error) {
                    reject(res.error);
                } else {
                    var user: User.User = {
                        facebookId: res.id,
                        name: res.name,
                        email: res.email
                    };
                    resolve(user);
                }
            });
        });
    }
}

import 'reflect-metadata';
import { getRepository } from 'typeorm';
import { User } from '../entities/user'
import { hashPassword } from '../hash-password'
import {INVALID_CREDENTIALS, EMAIL_NOT_FOUND} from '../errors';
import {TEN_MINUTES, ONE_WEEK, APP_SECRET} from '../consts';

var jwt = require('jsonwebtoken');

class LoginType {
    user: User;
    token: string;
  
    constructor( user: User ,token: string ) {
      this.user = user;
      this.token = token;
    }
  }

export default  {

    Mutation: {

        login: async (_, { data } ) => {
            
            const user: User = await getRepository(User).findOne({
            where: [
                { email: data.email }
                ]
            });
            
            if (!user) {
                throw new Error(EMAIL_NOT_FOUND);
            }

            if (user.password === hashPassword(data.password)) {

            var exp_time: number = TEN_MINUTES;

            if (data.rememberMe){
                exp_time = ONE_WEEK;
            };

            const token = jwt.sign({ userId: user.id }, APP_SECRET, {expiresIn: exp_time}); 

            return new LoginType(user, token);

            } else {
                throw new Error(INVALID_CREDENTIALS);
            };
        },
    }
}
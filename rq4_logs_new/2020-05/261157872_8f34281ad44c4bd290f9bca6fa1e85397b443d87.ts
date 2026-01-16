import { getRepository } from 'typeorm';
import * as jwt from 'jsonwebtoken';
import { User } from '../entities/user'
import { AUTHEN_ERROR } from '../errors';
import { APP_SECRET } from '../consts'

export default {

    Query: {
        
        async user (_, { users = 20 }, { request }){
            
            const token = request.headers.authorization
            
            try {
                jwt.verify(token, APP_SECRET);
            } catch(err) {
                throw new Error(AUTHEN_ERROR)
            }
            
            const v_users: User[] = await getRepository(User).find({ order: { name: 'ASC' }, take: users })

            return v_users

        },
    },
}
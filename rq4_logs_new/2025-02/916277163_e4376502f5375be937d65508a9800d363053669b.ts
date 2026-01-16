import { Error, ERROR_STATUS_CODE } from '@repo/services/error/error';
import { ERole } from '../shared';

import User from './user';
import type { UserConstructorParams, UserEntity } from './interface';

export default class AuthBusiness {
  initializeUser(params?: UserConstructorParams): User {
    return new User(params);
  }
  currentUser(user: UserEntity, authUser: UserEntity): User {
    this.validateCurrentUser(user.id, authUser);
    return new User({ ...user, clean: true });
  }
  validateCurrentUser(id: string, authUser: UserEntity) {
    if (id !== authUser.id && authUser.role !== ERole.ADMIN) {
      throw new Error({
        message: 'You are not authorized to access this feature',
        statusCode: ERROR_STATUS_CODE.UNAUTHORIZED_EXCEPTION,
      });
    }
  }
}
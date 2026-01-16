import {Injectable} from '@angular/core';
import {Role} from './auth.types';

const user = {
  name: 'Pawel Kaczmarek',
  roles: [Role.SPEAKER, Role.TRAINER, Role.DEVELOPER, Role.JEDIMASTER]
};

@Injectable()
export class AuthService {
  hasRole(role: Role) {
    return user.roles.find(userRole => userRole === role);
  }
}
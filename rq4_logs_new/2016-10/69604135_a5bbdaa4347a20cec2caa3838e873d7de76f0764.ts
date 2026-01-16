/**
 * Created by isa on 03/10/16.
 */

import { Injectable }     from '@angular/core';
import { CanActivate }    from '@angular/router';
import { Router } from '@angular/router';
import { UserService } from './user.service';

@Injectable()
export class LoginGuardService implements CanActivate {

  constructor(private userService: UserService, protected router: Router) {

  }

  canActivate() {
    if (this.userService.isLoggedIn()) {
      return true;
    }
    this.router.navigate(['/login']);
    return false;
  }
}
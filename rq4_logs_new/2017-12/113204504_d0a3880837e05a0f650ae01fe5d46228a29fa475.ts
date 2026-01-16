import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';
import {
  CanActivate,
  Router
} from '@angular/router';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/do';

import { AuthService } from '../../../../shared/service/auth';

@Injectable()
export class GuardService implements CanActivate {


  constructor(private authService: AuthService,
              private router: Router) {
  }

  canActivate(): Observable<boolean> {
    return this.authService.verifyToken()
      .map(response => response.result)
      .do(value => {
        if (!value) {
          this.authService.logout(() => {
            this.router.navigate(['/login']);
          });
        }
      });
  }
}
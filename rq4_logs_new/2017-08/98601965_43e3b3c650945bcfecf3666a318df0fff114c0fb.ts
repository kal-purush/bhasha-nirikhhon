import {CanActivate, Router} from '@angular/router';
import { Injectable } from '@angular/core';
import {TokenService} from '../service/token.service';
import swal from 'sweetalert2';

@Injectable()
export default class LoggedGuard implements CanActivate {
  constructor(private tokenService: TokenService,
              private router: Router) {}
  canActivate() {
    if (this.tokenService.isLogged()) {
      return true;
    }
    swal('Thông báo', 'Mời bạn đăng nhập', 'error');
    this.router.navigate(['/login']);
    return false;
  }
}
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot } from '@angular/router';
import { Injectable } from '@angular/core';
import { TokenService} from '../service/token.service';
import swal from 'sweetalert2';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class LoggedGuard implements CanActivate {
  constructor(private tokenService: TokenService,
              private router: Router) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
    if (this.tokenService.isLogged()) {
      return true;
    }
    swal('Thông báo', 'Mời bạn đăng nhập', 'error');
    this.router.navigate(['/login']);
    return false;
  }
}
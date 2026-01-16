import { Injectable } from '@angular/core';
import {
CanActivate,
ActivatedRouteSnapshot,
RouterStateSnapshot
} from '@angular/router';
import { Observable } from 'rxjs/Observable';
import { AuthService } from '../app_services/auth.service';
import { Router } from '@angular/router';

@Injectable()
export class CheckDevGuard implements CanActivate {
	constructor(private authService: AuthService, private router: Router) {}

	canActivate(
		next: ActivatedRouteSnapshot,
		state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
            const userLoggedIn = this.authService.getUser();
			if (userLoggedIn && userLoggedIn[0] == 'Q'){
				this.router.navigate(['/profile']);
				return false;
			}
				return true;
		}
}
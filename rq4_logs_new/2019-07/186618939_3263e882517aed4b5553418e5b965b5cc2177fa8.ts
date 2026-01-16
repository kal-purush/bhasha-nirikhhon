import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LoginGuard implements CanActivate {
	constructor(private router: Router) {}

	canActivate(
		next: ActivatedRouteSnapshot,
		state: RouterStateSnapshot): boolean {
		console.log('VERIFICANDO ROTAS!'+state.url);
		let url: string = state.url;
		return this.verifyLogin(url);
	}

	verifyLogin(url: string): boolean {
		if(localStorage.getItem("token")){
			if (url == '/login'){
				this.router.navigate(['/clientes']);
			}
			return true;
		}

		if (url != '/login'){
			this.router.navigate(['/login']);
		}
		return true;
	}
  
}
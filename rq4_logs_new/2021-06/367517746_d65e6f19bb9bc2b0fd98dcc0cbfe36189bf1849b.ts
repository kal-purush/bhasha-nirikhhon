import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivate, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ClienteAdminGuard implements CanActivate {
  constructor(private router: Router){}
  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): boolean {
      if (localStorage.getItem('Rol') === '3' || localStorage.getItem('Rol') === '1'  && localStorage.getItem('token') !== null){
        return true;
      }else {
        this.router.navigate(['usuario/login']);
        return true;
    }
  }


}
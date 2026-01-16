import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree } from '@angular/router';
import { Observable } from 'rxjs';
import { map, take, tap } from 'rxjs/operators';
import {LoginArtistasService} from '../../services/login-artistas.service';

@Injectable({
  providedIn: 'root'
})
export class AdminGuard implements CanActivate {
  constructor(private authSvc: LoginArtistasService){}
  canActivate(): Observable<boolean> | Promise<boolean> | boolean {
    return this.authSvc.user$.pipe(
      take(1),
      map((user)=> user && this.authSvc.isAdmin(user)),
      tap( canEdit =>{
        if(!canEdit){
          window.alert('Acceso denegado, Para ingresar aquí debe ser un usuario tipo ADMINISTRADOR');
        }
      })
    );
  }
  
}
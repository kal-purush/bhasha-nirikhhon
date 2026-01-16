import {Injectable} from '@angular/core';
import {ActivatedRouteSnapshot, Resolve, RouterStateSnapshot} from "@angular/router";
import {AppUser} from '../models/app-user';
import {Observable} from 'rxjs';
import {UserService} from './user-service.service';

@Injectable({
  providedIn: 'root'
})
export class ResolverService implements Resolve<AppUser> {

  constructor(private userService: UserService) {
  }

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<AppUser> | Promise<AppUser> | AppUser {
    return this.userService.getUser(this.userService.CurrentUser._id);
  }
}
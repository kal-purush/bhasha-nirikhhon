import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { OrganizationService } from '../services/organization.service';
import { take, map } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class OrganizationGuard implements CanActivate {

  constructor(
    private router: Router,
    private org: OrganizationService
  ) { }


  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    console.log('params:', next.url);
    if (next.url.length == 0) {
      return true;
    }
    if (next.url[0].path == "organization") {
      return true;
    }
    return this.org.organization.pipe(
      take(1),
      map(organization => {
        console.log("Organization guard called!");
        console.log(organization);
        if (!organization) {
          this.router.navigateByUrl('/app/organization');
          return false;
        }
        return true;
      })
    )
  }

  canActivateChild(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot
  ) {
    return this.canActivate(next, state);
  }

}
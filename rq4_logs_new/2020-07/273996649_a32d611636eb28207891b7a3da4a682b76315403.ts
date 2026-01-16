import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { OrganizationService } from '../services/organization.service';
import { take, map } from 'rxjs/operators';
import { Organization } from '../services/organization';

@Injectable({
  providedIn: 'root'
})
export class OrganizationAdminGuard implements CanActivate {

  constructor(
    private router: Router,
    private org: OrganizationService,
  ) { }

  canActivate(
    next: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): Observable<boolean | UrlTree> | Promise<boolean | UrlTree> | boolean | UrlTree {
    var id = parseInt(next.paramMap.get("organization"));
    return this.org.organizations.pipe(
      take(1),
      map((organizations: Array<Organization>) => {
        var current = organizations.find(e => e.id == id);
        this.org.current.set(current).subscribe();
        if (!current.admin_flag) {
          this.router.navigateByUrl('/app/home');
          return false;
        }
        return true;
      })
    );
  }

}
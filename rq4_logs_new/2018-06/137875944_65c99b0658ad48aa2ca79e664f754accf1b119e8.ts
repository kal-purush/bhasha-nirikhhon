import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Observable } from 'rxjs/Observable';
import { CanActivate, Router, ActivatedRouteSnapshot } from '@angular/router';
import { DataService } from './data.service';

@Injectable()
export class AuthGuardService implements CanActivate {
  status: Boolean;
  constructor(
    private dataService: DataService,
    private router: Router) {
    this.dataService.getStatus().subscribe(
      data => this.status = data
    );
  }
  
  canActivate(route: ActivatedRouteSnapshot): boolean {
    // If logged in and the user is an admin
    if (this.status && route.data.role === "admin") {
      return true;
    } else {
      this.router.navigate(['/one']);
      return false;
    }
  }
}
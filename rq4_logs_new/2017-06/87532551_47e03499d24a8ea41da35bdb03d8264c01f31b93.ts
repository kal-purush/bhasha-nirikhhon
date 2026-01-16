import { Injectable } from '@angular/core';
import { ActivatedRouteSnapshot, Resolve, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs';
import { Dashboard }  from '../dashboard.component';

@Injectable()
export class DashboardResolver implements Resolve<Dashboard> {

    constructor() {
    }

    resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any> | Promise<any> | any {
        return this.loadDashboard();
    }

    loadDashboard(): Promise<any> {
        return new Promise ((resolve, reject) => {
            let params = JSON.parse(localStorage.getItem('dashboard'));
            resolve(params);
        });
    }
}
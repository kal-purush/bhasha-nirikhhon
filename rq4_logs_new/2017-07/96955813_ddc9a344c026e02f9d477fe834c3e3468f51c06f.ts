import { Injectable } from '@angular/core';
import { Router, Resolve, RouterStateSnapshot,
         ActivatedRouteSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';

import { GamerInfoService } from './gamer-info.service';

@Injectable()
export class GamerSummaryResolver implements Resolve<any> {
    constructor(private summaryService: GamerInfoService , private router: Router) {}

    resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any> {
      const network = route.params['network'];
      const id = route.params['membershipId'];
      return this.summaryService.summary(network, id);
    }
}
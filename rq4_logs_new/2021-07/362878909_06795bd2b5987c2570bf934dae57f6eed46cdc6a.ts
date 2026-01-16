import { Injectable } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs';

// SERVICE
import { GlobalService, SidebarService } from '@services/private';

// PACKAGE
import { TranslateService } from '@ngx-translate/core';

@Injectable({ providedIn: 'root' })
export class SidebarResolver implements Resolve<any> {
  constructor(
    private globalService: GlobalService,
    private sidebarService: SidebarService,
    private translateService: TranslateService
  ) {}

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<any> | Promise<any> | any {
    const menu = this.sidebarService.getSidebars();
    this.sidebarService.changeSidebar(menu);
  }
}
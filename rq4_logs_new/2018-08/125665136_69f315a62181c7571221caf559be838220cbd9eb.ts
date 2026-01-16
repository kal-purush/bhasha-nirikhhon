import { Injectable, NgModule } from '@angular/core';
import { Resolve, ActivatedRouteSnapshot } from '@angular/router';

import { AuthService } from '@rd/core';
import { Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { CoreAuthService } from '@rd/core';
import { LogoffResolve } from './logoff.resolve';
import { LogoutRoutingModule } from './logoff-routes';
import { LogoffComponent } from './logoff.component';

@NgModule({
    declarations: [
        LogoffComponent
    ],
    imports: [
        CommonModule,
        //   LogoutRoutingModule,
    ],
    providers: [
        LogoffResolve
    ]
})
export class LogoffModule { }
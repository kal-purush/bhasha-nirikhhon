import { NgModule } from '@angular/core';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { AdminLoginComponent } from './admin-login.component';
import { AppHeaderModule } from '../share/app-header/app-header.module';
import { AppFooterModule } from '../share/app-footer/app-footer.module';

import { routing } from './admin-login.routing';

@NgModule({
    declarations: [
        AdminLoginComponent,
    ],
    imports: [
        NgZorroAntdModule,
        AppHeaderModule,
        AppFooterModule,
        routing,
    ],
})
export class AdminLoginModule { }
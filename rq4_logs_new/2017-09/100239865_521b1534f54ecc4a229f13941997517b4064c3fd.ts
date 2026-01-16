import { NgModule } from '@angular/core';
// import { CommonModule } from '@angular/common';
// import { FormsModule } from '@angular/forms';
import { AppHeaderModule } from '../share/app-header/app-header.module';
import { AppFooterModule } from '../share/app-footer/app-footer.module';
import { NgZorroAntdModule } from 'ng-zorro-antd';

import { CategoryComponent } from './category.component';

import { routing } from './category.routing';

@NgModule({
    declarations: [
        CategoryComponent,
    ],
    imports: [
        NgZorroAntdModule,
        // CommonModule,
        // FormsModule,
        AppHeaderModule,
        AppFooterModule,
        routing,
    ],
})
export class CategoryModule { }
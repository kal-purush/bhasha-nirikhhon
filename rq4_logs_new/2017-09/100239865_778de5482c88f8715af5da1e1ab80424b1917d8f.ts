import { NgModule } from '@angular/core';
import { AppHeaderModule } from '../share/app-header/app-header.module';
import { HomeComponent } from './home.component';
import { ArticleRowComponent } from './article-row/article-row.component';
import { NgZorroAntdModule } from 'ng-zorro-antd';

import { routing } from './home.routing';

@NgModule({
    declarations: [
        HomeComponent,
        ArticleRowComponent,
    ],
    imports: [
        NgZorroAntdModule,
        AppHeaderModule,
        routing
    ],
})
export class HomeModule { }
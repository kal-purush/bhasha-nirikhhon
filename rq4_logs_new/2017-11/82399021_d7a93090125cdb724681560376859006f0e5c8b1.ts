import {NgModule} from '@angular/core';

import {SharedModule} from '@app/shared/shared.module';
import {PageRoutingModule} from '@app/page/page.routing';
import {PageComponent} from '@app/page/page.component';

@NgModule({
    imports: [
        SharedModule,
        PageRoutingModule
    ],
    declarations: [
        PageComponent
    ]
})
export class PageModule { }
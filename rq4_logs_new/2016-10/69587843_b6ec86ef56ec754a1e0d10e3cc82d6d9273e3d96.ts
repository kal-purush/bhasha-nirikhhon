import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { AboutComponent } from './about.component';
import { AboutRouting } from './about.routing';

@NgModule({
    imports: [CommonModule, AboutRouting],
    declarations: [AboutComponent]
})
export class AboutModule {}
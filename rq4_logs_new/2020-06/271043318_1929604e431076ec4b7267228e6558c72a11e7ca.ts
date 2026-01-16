import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { MenuHeaderPageRoutingModule } from './menu-header-routing.module';

import { MenuHeaderPage } from './menu-header.page';

@NgModule({
    imports: [
        CommonModule,
        FormsModule,
        IonicModule,
        MenuHeaderPageRoutingModule
    ],
    exports: [
        MenuHeaderPage
    ],
    declarations: [MenuHeaderPage]
})
export class MenuHeaderPageModule {}
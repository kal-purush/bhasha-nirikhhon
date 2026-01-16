import { IonicModule } from '@ionic/angular';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Tab1Page } from './tab1.page';

import { Tab1PageRoutingModule } from './tab1-routing.module';
import { ButtomHomeComponent } from '../components/buttom-home/buttom-home.component';
import { MenuListComponent } from '../components/menu-list/menu-list.component';

@NgModule({
  imports: [
    IonicModule,
    CommonModule,
    FormsModule,
    Tab1PageRoutingModule
  ],
  declarations: [Tab1Page, ButtomHomeComponent, MenuListComponent]
})
export class Tab1PageModule {}
import { IonicModule } from '@ionic/angular';
import { RouterModule } from '@angular/router';
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Tab4Page } from './tab4.page'
import { Tab3PageRoutingModule } from './tab4-routing.module';
import { MenuListComponent } from '../components/menu-list/menu-list.component';

@NgModule({
  imports: [
    IonicModule,
    CommonModule,
    FormsModule,
    RouterModule.forChild([{ path: '', component: Tab4Page }]),
    Tab3PageRoutingModule,
  ],
  declarations: [Tab4Page, MenuListComponent]
})
export class Tab4PageModule {}
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { DirectCostsEditPageRoutingModule } from './direct-costs-edit-routing.module';

import { DirectCostsEditPage } from './direct-costs-edit.page';
import { ModalEditPage } from '../modal-edit/modal-edit.page';
import { ComponentsModule } from 'src/app/components/components.module';
import { ModalInsumoPage } from '../modal-insumo/modal-insumo.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    ComponentsModule,
    DirectCostsEditPageRoutingModule
  ],
  declarations: [DirectCostsEditPage,],
  entryComponents:[ModalEditPage, ModalInsumoPage]

})
export class DirectCostsEditPageModule {}
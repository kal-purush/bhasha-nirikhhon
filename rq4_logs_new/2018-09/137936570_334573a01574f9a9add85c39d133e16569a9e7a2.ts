import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ListarConsultasPage } from './listar-consultas';

@NgModule({
  declarations: [
    ListarConsultasPage,
  ],
  imports: [
    IonicPageModule.forChild(ListarConsultasPage),
  ],
})
export class ListarConsultasPageModule {}
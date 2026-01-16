import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { ListPacientePage } from './list-paciente';

@NgModule({
  declarations: [
    ListPacientePage,
  ],
  imports: [
    IonicPageModule.forChild(ListPacientePage),
  ],
})
export class ListPacientePageModule {}
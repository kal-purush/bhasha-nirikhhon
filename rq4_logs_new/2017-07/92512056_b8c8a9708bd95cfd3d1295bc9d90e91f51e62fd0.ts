import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { GraficoPorAlunoPage } from './grafico-por-aluno';

@NgModule({
  declarations: [
    GraficoPorAlunoPage,
  ],
  imports: [
    IonicPageModule.forChild(GraficoPorAlunoPage),
  ],
  exports: [
    GraficoPorAlunoPage
  ]
})
export class GraficoPorAlunoPageModule {}
import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { InformacoesPageRoutingModule } from './informacoes-routing.module';

import { InformacoesPage } from './informacoes.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    IonicModule,
    InformacoesPageRoutingModule
  ],
  declarations: [InformacoesPage]
})
export class InformacoesPageModule {}
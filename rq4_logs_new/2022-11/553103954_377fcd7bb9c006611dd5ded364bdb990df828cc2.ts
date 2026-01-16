import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MiPerfilComponent } from './mi-perfil.component';
import { MisHorariosComponent } from '../mis-horarios/mis-horarios.component';
import { Spinner2Component } from '../spinner2/spinner2.component';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { BoolToSinoPipe } from 'src/app/shared/Pipes/bool-to-sino.pipe';
import { AppModule } from 'src/app/app.module';
import { TimestampToDatePipe } from 'src/app/shared/Pipes/timestamp-to-date.pipe';
import { SharedModule } from 'src/app/shared/shared.module';
import { TurnosRoutingModule } from './mi-perfil-routing.module';



@NgModule({
  declarations: [
    MiPerfilComponent,
    MisHorariosComponent,
    Spinner2Component
  ],
  imports: [
    CommonModule,
    MatProgressSpinnerModule,
    ReactiveFormsModule,
    MatButtonModule,
    FormsModule,
    SharedModule,
    TurnosRoutingModule
  ],
  providers: [
    BoolToSinoPipe,
    TimestampToDatePipe
  ]
})
export class MiPerfilModule { }
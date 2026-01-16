import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { AlumnoPageRoutingModule } from './tareas-routing.module';

import { TareasPage } from './tareas.page';
import { TareaComponent } from 'src/app/components/tarea/tarea.component';


@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    AlumnoPageRoutingModule
  ],
  declarations: [TareasPage,TareaComponent],
})
export class TareasPageModule {}
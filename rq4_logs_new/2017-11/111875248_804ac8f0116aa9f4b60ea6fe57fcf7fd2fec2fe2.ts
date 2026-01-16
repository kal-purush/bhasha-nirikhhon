import { NgModule }             from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { PersonasCrudComponent } from './personas-crud/personas-crud.component';
import { ProyectosCrudComponent } from './proyectos-crud/proyectos-crud.component';
import { TareasComponent } from './tareas/tareas.component';

const routes: Routes = [
  //{ path: '', redirectTo: '/dashboard', pathMatch: 'full' },
  { path: 'personas', component: PersonasCrudComponent },
  { path: 'proyectos', component: ProyectosCrudComponent },
  { path: 'tareas', component: TareasComponent },
  { path: '', component: PersonasCrudComponent },
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})

export class AppRoutingModule { }
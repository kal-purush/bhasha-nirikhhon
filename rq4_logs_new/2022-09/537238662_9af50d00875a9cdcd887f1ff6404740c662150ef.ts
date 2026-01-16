import { AgregarMateriasComponent } from './materias/agregarMaterias/agregar-materias/agregar-materias.component';
import { TablaMateriasComponent } from './materias/tablaMaterias/tabla-materias/tabla-materias.component';
import { AgregarDocentesComponent } from './docentes/agregarDocentes/agregar-docentes/agregar-docentes.component';
import { TablaDocentesComponent } from './docentes/tablaDocentes/tabla-docentes/tabla-docentes.component';
import { AuthGuard } from './auth.guard';
import { LoginFormularioComponent } from './public/login-formulario/login-formulario.component';
import { NgModule} from '@angular/core';
import { RouterModule, Routes } from '@angular/router';


const routes: Routes = [
  {path: '', component:  TablaDocentesComponent},
  //{path: 'login', component:  LoginFormularioComponent},//

  {path:'mostrarDocentes', component:TablaDocentesComponent},
  {path:'agregarDocentes', component:AgregarDocentesComponent},
  {path:'modificarDocentes/:id', component:AgregarDocentesComponent},

  {path:'mostrarMaterias', component:TablaMateriasComponent},
  {path:'agregarMaterias', component:AgregarMateriasComponent},
  {path:'modificarMaterias/:id', component:AgregarMateriasComponent},


];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
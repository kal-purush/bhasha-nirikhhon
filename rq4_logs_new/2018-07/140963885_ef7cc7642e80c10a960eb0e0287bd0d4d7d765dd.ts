import {Routes} from "@angular/router";
import {HomeComponent} from "./Componentes/home/home.component";
import {LoginComponent} from "./Componentes/login/login.component";

export const routes: Routes = [
  {
    path: 'home',
    component: HomeComponent},
  {
    path: 'login',
    component: LoginComponent}
  /*{
    path: 'modeloPaciente/:idPaciente/modeloMedicamento/:idMedicamento',
    component: ModeloMedicamentoComponent },
  {
    path: 'modeloPaciente/:idPaciente',
    component: ModeloPacienteComponent,
    children: [{
      path: 'modeloMedicamento/:idMedicamento',
      component: ModeloMedicamentoComponent
    }]
  }*/
];
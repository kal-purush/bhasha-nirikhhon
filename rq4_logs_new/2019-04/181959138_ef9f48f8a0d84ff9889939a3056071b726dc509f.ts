import { NgModule } from '@angular/core';
import{Routes, RouterModule}from'@angular/router';
import { PaisListComponent } from './pais-list/pais-list.component';
import { PaisFormComponent } from './pais-form/pais-form.component';

const routes: Routes = [
  {path:"pais", component:PaisListComponent},
  {path:"pais/novo",component:PaisFormComponent}
];

@NgModule({
  declarations: [],
  imports: [RouterModule.forRoot(routes)],
  exports:[RouterModule]
})
export class AppRoutingModule { }
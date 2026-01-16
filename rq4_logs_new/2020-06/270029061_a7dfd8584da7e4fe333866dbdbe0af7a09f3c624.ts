

import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router'; // CLI imports router

import { PageNotFoundComponent } from './page-not-found.component';
import { AppComponent } from './app.component';
import { DashboardComponent } from './components/dashboard/dashboard.component';
import { EmployeesComponent } from './components/employees/employees.component';
import { UsersComponent } from './components/users/users.component';
import { AreasComponent } from './components/areas/areas.component';
import { MaterialsComponent } from './components/materials/materials.component';
import { CompetencesComponent } from './components/competences/competences.component';
import { ProceduresComponent } from './components/procedures/procedures.component';
import { TypologiesComponent } from './components/typologies/typologies.component';

const routes: Routes = [
  { path: 'dashboard',
component: DashboardComponent
},
{ path: 'employees',
component: EmployeesComponent
},
{	path: '',
redirectTo: 'dashboard',
pathMatch: 'full'
},
{ path: 'users',
component: UsersComponent
},
{ path: 'competences',
component: CompetencesComponent
},
{ path: 'procedures',
component: ProceduresComponent
},
{ path: 'areas',
component: AreasComponent
},

{ path: 'materials',
component: MaterialsComponent
},
{ path: 'typologies',
component: TypologiesComponent
},
{
path: '**',
component: PageNotFoundComponent
}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}
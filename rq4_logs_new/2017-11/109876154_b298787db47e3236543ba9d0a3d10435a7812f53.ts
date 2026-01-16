import {RouterModule, Routes} from '@angular/router';
import {NgModule} from '@angular/core';
import {DemoComponent} from './service_test/service_test.component';
import {AddUserComponent} from './add-user/add-user.component';
import {UserComponent} from './user/user.component';

const routes: Routes = [
  {path: '', redirectTo: '/demo', pathMatch: 'full'},
  {path: 'demo', component: DemoComponent},
  {path: 'add', component: AddUserComponent},
  {path: 'edit/:id', component: UserComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})

export class AppRoutingModule {
}
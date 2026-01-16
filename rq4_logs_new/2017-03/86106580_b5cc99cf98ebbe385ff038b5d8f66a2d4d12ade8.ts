    import { NgModule }             from '@angular/core';
    import { RouterModule, Routes } from '@angular/router';
    import  {HomeComponent,RegisterComponent,LoginComponent,TasksComponent,UsersComponent } from './components';
    const routes: Routes = [
      { path: '',  component: HomeComponent },
      { path: 'home',   redirectTo: '',  pathMatch: 'full'},
      { path: 'register', component: RegisterComponent },
      { path: 'login',     component: LoginComponent },
      { path: 'tasks',     component: TasksComponent },
      { path: 'users',     component: UsersComponent }
    ];
    @NgModule({
      imports: [ RouterModule.forRoot(routes) ],
      exports: [ RouterModule ]
    })
    export class AppRoutingModule {}
import {NgModule} from '@angular/core';
import {Routes, RouterModule} from '@angular/router';
import {AuthComponent} from './modules/auth/auth.component';
import {MainComponent} from './modules/main/main.component';
import {AccessGuard} from './common/guards/access.guard';
import {TodoListComponent} from './modules/main/modules/todo-list/todo-list.component';
import {AddTodoComponent} from './modules/main/modules/add-todo/add-todo.component';
import {IdExistGuard} from './common/guards/id-exist.guard';


const routes: Routes = [
  {
    path: '',
    pathMatch: 'full',
    redirectTo: 'auth'
  },
  {
    path: 'auth',
    component: AuthComponent
  },
  {
    path: 'main',
    component: MainComponent,
    canActivate: [AccessGuard],
    children: [
      {
        path: '',
        pathMatch: 'full',
        redirectTo: 'list'
      },
      {
        path: 'list',
        component: TodoListComponent
      },
      {
        path: 'add',
        component: AddTodoComponent
      },
      {
        path: 'edit/:id',
        component: AddTodoComponent,
        resolve: [IdExistGuard]
      },
      {
        path: '**',
        redirectTo: '/auth'
      }
    ]
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {
}
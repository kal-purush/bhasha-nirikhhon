import {ModuleWithProviders} from '@angular/core';
import {Routes, RouterModule} from '@angular/router';
import {TaskListComponent} from "./task/task-list.component";
import {TaskEditComponent} from "./task/task-edit.component";
import {TaskNewComponent} from "./task/task-new.component";

const appRoutes: Routes = [
    {
        path: '',
        redirectTo: '/tasks/list',
        pathMatch: 'full'
    },
    {
        path: 'tasks/list',
        component: TaskListComponent
    },
    {
        path: 'tasks/new',
        component: TaskNewComponent
    },
    {
        path: 'tasks/:id/edit',
        component: TaskEditComponent
    }
];

export const routing: ModuleWithProviders = RouterModule.forRoot(appRoutes);
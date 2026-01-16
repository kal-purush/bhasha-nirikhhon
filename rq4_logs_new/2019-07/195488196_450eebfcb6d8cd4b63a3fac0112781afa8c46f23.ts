import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ApiManageComponent } from './api-manage/api-manage.component';
import { ApiSetupComponent } from './api-setup/api-setup.component';

const routes: Routes = [
    {
        path: '',
        component: ApiSetupComponent,
        pathMatch: 'full'
    },
    {
        path: 'manage',
        component: ApiManageComponent
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    providers: [],
    exports: [RouterModule]
})
export class ApiManageRoutingModule {}
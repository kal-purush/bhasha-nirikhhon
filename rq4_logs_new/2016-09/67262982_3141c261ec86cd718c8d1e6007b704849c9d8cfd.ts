import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { VolumesComponent } from './volumes.component';
import { DashboardComponent } from './dashboard.component';
import { VolumeDetailComponent } from './volume-detail.component';

const appRoutes: Routes = [
    {
        path: 'volumes',
        component: VolumesComponent
    },
    {
        path: 'dashboard',
        component: DashboardComponent
    },
    {
        path: '',
        redirectTo: '/dashboard',
        pathMatch: 'full'
    },
    {
        path: 'detail/:name',
        component: VolumeDetailComponent
    }
];

export const routing: ModuleWithProviders = RouterModule.forRoot(appRoutes);
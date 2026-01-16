import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { Scene } from './scene';

const appRoutes: Routes = [
    { path: 'play/:sceneNumber', component: Scene

    }]

export const appRoutingProviders: any[] = [

];

export const routing: ModuleWithProviders = RouterModule.forRoot(appRoutes);
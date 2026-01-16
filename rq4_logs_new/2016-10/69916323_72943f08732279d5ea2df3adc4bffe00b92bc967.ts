// Imports
// Deprecated import
// import { provideRouter, RouterConfig } from '@angular/router';

import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

// Components
import { HomeComponent } from './Components/Home/home.component';


// Component route configuration file
import { loginRoutes } from './Components/MembershipProvider/membership-provider.routes';

export const routes: Routes = [
    { path: '', component: HomeComponent},
    ...loginRoutes  // Add login routes form different file
];

export const Routing: ModuleWithProviders = RouterModule.forRoot(routes);
import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { FormComponent } from './form/form.component';

import { MattswhackkywonderlandComponent } from './mattswhackkywonderland/mattswhackkywonderland.component'

const routes: Routes = [
	{ path: 'form', component: FormComponent },
	{ path: "whatever", component: MattswhackkywonderlandComponent }
];

export const appRoutes: ModuleWithProviders = RouterModule.forRoot(routes);
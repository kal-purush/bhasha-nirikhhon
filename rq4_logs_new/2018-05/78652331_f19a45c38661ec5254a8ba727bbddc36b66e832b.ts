import { RouterModule, Routes } from '@angular/router';
import { ModuleWithProviders } from '@angular/core';

import { SearchResultComponent } from '../search-result/search-result.component';

export const routes: Routes = [
  { path: '', component: SearchResultComponent }
];

export const routing: ModuleWithProviders = RouterModule.forChild(routes);
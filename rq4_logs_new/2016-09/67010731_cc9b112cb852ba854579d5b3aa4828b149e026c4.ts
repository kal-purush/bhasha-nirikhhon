import { Routes, RouterModule } from '@angular/router';
import { AssetTrackerComponent } from './asset-tracker/asset-tracker.component';
import { IssueAdminComponent } from './issue-admin/issue-admin.component';
import { IssuesComponent } from './issues/issues.component';

const appRoutes: Routes = [
  {
    path: '',
    redirectTo: 'assettracker',
    pathMatch: 'full'
  },
    {
    path: 'assettracker',
    component: AssetTrackerComponent
  },
    {
    path: 'issues/admin',
    component: IssueAdminComponent
  },
    {
    path: 'issues',
    component: IssuesComponent
  },
];

export const routing = RouterModule.forRoot(appRoutes);
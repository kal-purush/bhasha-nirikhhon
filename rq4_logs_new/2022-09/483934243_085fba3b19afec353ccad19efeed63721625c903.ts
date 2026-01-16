import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { LandingViewerComponent } from './landing-viewer/landing-viewer.component';
import { MgsFanpageComponent } from './mgs-fanpage/mgs-fanpage.component';
import { SampleFormComponent } from './sample-form/sample-form.component';

const routes: Routes = [
    {
        path: 'landingpages',
        component: LandingViewerComponent,
        children: [
            {
                path: 'mgs',
                component: MgsFanpageComponent
            },
            {
                path: 'form',
                component: SampleFormComponent
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
  })
  export class landingPagesRoutingModule { }
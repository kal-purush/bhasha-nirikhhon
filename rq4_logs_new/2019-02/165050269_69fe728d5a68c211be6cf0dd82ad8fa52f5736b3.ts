import { NgModule } from '@angular/core';

import { SharedModule } from '@app/shared';
import * as views from './views';
import { RouterModule } from '@angular/router';

@NgModule({
  declarations: [
    // Views
    views.AuthorizedViewComponent,
    views.LoginViewComponent,
    views.NotFoundViewComponent
  ],
  imports: [SharedModule, RouterModule],
  exports: [
    // Views
    views.AuthorizedViewComponent,
    views.LoginViewComponent,
    views.NotFoundViewComponent
  ]
})
export class BaseModule {}
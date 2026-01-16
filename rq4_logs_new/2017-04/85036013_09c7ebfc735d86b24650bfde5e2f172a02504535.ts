import { NgModule } from '@angular/core';
import { Framing } from '@framing/ng-core';
import { SharedModule } from 'app/shared/shared.module';

import { AppBarTitleComponent } from './app-bar-title.component';

/**
 * Example module containing AppFramer view overrides
 */
@NgModule(Framing((framing) => framing
  .import(SharedModule)
  .declarationsAndEntryComponents([
    AppBarTitleComponent,
  ]),
))
export class AppViewModule {}
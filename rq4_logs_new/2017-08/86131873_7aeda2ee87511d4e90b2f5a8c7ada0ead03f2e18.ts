import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { ViewportService } from './shared/services/viewport/viewport.service';
import { PopupService } from './shared/services/popup/popup.service';

@NgModule({
  providers: [
    ViewportService,
    PopupService
  ],
})
export class CoreModule { }